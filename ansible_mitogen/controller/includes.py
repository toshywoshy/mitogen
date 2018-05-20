# (c) 2012-2014, Michael DeHaan <michael.dehaan@gmail.com>
#
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import
import logging

import ansible.errors
import ansible.executor.task_result
import ansible.playbook.block
import ansible.playbook.helpers
import ansible.playbook.task

from ansible.module_utils import six
from ansible.module_utils._text import to_text
from ansible.playbook.included_file import IncludedFile


LOG = logging.getLogger(__name__)


def _make_noop_task(loader):
    """
    Construct a :class:`ansible.playbook.task.Task` representing a "meta: noop"
    task.

    :param ansible.parsing.dataloader.DataLoader loader:
    """
    task = ansible.playbook.task.Task()
    task.action = 'meta'
    task.args['_raw_params'] = 'noop'
    task.set_loader(loader)
    return task


def _replace_with_noop(target, loader):
    """
    :param ansible.parsing.dataloader.DataLoader loader:
    """
    result = []
    for el in target:
        if isinstance(el, ansible.playbook.task.Task):
            result.append(_make_noop_task(loader))
        elif isinstance(el, ansible.playbook.block.Block):
            result.append(_make_noop_block_from(el, el._parent, loader))
    return result


def _make_noop_block_from(block, parent, loader):
    """
    Construct a block with the same shape and task count as `original`, but
    with each task replaced by a no-op task.

    :param ansible.playbook.block.Block block:
        Block to mimic.
    :param ansible.playbook.block.Block parent:
        Parent of new block.
    :param ansible.parsing.dataloader.DataLoader loader:
        Loader to assign to new tasks within the block.
    """
    new = ansible.playbook.block.Block(parent_block=parent)
    new.block = _replace_with_noop(block.block, loader)
    new.always = _replace_with_noop(block.always, loader)
    new.rescue = _replace_with_noop(block.rescue, loader)
    return new


def _copy_included_file(original):
    '''
    A proven safe and performant way to create a copy of an included file
    '''
    new = original._task.copy(exclude_parent=True)
    new._parent = original._task._parent
    new_vars = new.vars.copy()
    new_vars.update(original._args)
    new.vars = new.vars
    return new


class Error(Exception):
    pass


class Parser(object):
    def __init__(self, tqm, play_context, var_manager, iterator):
        self._tqm = tqm
        self._play_context = play_context
        self._var_manager = var_manager
        self._iterator = iterator

    not_list_msg = "included task files must contain a list of tasks"
    both_styles_msg = (
        "Include tasks should not specify tags in more than one way (both "
        "via args and directly on the task). Mixing tag specify styles is "
        "prohibited for whole import hierarchy, not only for single import "
        "statement"
    )

    def _parse_tags(self, ifile, data):
        """
        Pop tags out of the include args, if they were specified there, and
        assign them to the include. If the include already had tags specified,
        we raise an error so that users know not to specify them both ways.
        """
        tags = ifile._task.vars.pop('tags', [])
        if isinstance(tags, six.string_types):
            tags = tags.split(',')
        if len(tags) > 0:
            if len(ifile._task.tags) > 0:
                raise ansible.errors.AnsibleParserError(
                    self.both_styles_msg,
                    obj=ifile._task._ds
                )
            display.deprecated(
                "You should not specify tags in the include parameters. "
                "All tags should be specified using the task-level option"
            )
            ifile._task.tags = tags

    def _load_blocks(self, ifile, data, is_handler):
        return ansible.playbook.helpers.load_list_of_blocks(
            data,
            play=self._iterator._play,
            parent_block=None,
            task_include=_copy_included_file(ifile),  # TODO WHY
            role=ifile._task._role,
            use_handlers=is_handler,
            loader=self._tqm.get_loader(),
            var_manager=self._var_manager,
        )

    def _load(self, ifile, is_handler=False):
        '''
        Loads an included YAML file of tasks, applying the optional set of
        variables.
        '''
        LOG.debug("loading included file: %s", ifile._filename)

        try:
            data = self.tqm.get_loader().load_from_file(ifile._filename)
            if data is None:
                return []
            if not isinstance(data, list):
                raise ansible.errors.AnsibleError(self.not_list_msg)

            self._parse_tags(ifile, data)
            blocks = self._load_blocks(ifile, data, is_handler)
            # since we skip incrementing the stats when the task result is
            # first processed, we do so now for each host in the list
            for host in ifile._hosts:
                self._tqm._stats.increment('ok', host.name)
        except ansible.errors.AnsibleError as e:
            for host in ifile._hosts:
                self._tqm._stats.increment('failures', host.name)
                self._mark_host_failed(ifile, host, to_text(e))
            return []

        # finally, send the callback and return the list of blocks loaded
        self._tqm.send_callback('v2_playbook_on_include', ifile)
        LOG.debug("done processing included file")
        return blocks

    def _mark_host_failed(self, ifile, host, reason):
        """
        Mark all of the hosts including this file as failed, send callbacks,
        and increment stats.
        """
        self._iterator.mark_host_failed(host)
        self._tqm._failed_hosts[host.name] = True
        self._tqm.send_callback('v2_runner_on_failed',
            ansible.executor.task_result.TaskResult(
                host=host,
                task=ifile._task,
                return_data={
                    'failed': True,
                    'reason': reason,
                }
            )
        )

    def _do_block(self, task, ifile, host_blocks, block):
        task_vars = self._var_manager.get_vars(
            play=self._iterator._play,
            task=block._parent
        )
        for host in host_blocks:
            if host in ifile._hosts:
                LOG.debug("filtering new block on tags")
                host_blocks[host].append(
                    block.filter_tagged_tasks(self._play_context, task_vars)
                )
                LOG.debug("done filtering new block on tags")
            else:
                loader = self._iterator._play._loader
                noop = _make_noop_block_from(final, task._parent, loader)
                host_blocks[host].append(noop)

    def _process_include_file(self, ifile, task, host_blocks):
        if ifile._is_role:
            new_ir = _copy_included_file(ifile)  # TODO WHY
            blocks, handler_blocks = new_ir.get_block_list(
                play=self._iterator._play,
                var_manager=self._var_manager,
                loader=self._tqm.get_loader(),
            )
            self._tqm.update_handler_list([
                handler
                for handler_block in handler_blocks
                for handler in handler_block.block
            ])
        else:
            blocks = self._load_included_file(ifile, iterator=self._iterator)

        LOG.debug("iterating over blocks loaded from include file")
        for block in blocks:
            self._do_block(task, ifile, host_blocks, block)
        LOG.debug("done iterating over blocks loaded from include file")

    def parse(self, task, results, hosts):
        try:
            ifiles = IncludedFile.process_include_results(
                results,
                iterator=self._iterator,
                loader=self._tqm.get_loader(),
                variable_manager=self._var_manager
            )
        except ansible.errors.AnsibleError as e:
            # this is a fatal error, abort regardless of block state.
            raise Error(e)

        host_blocks = dict((host, []) for host in hosts)
        for ifile in ifiles:
            # included hosts get the task list while those excluded get an
            # equal-length list of noop tasks, to make sure that they continue
            # running in lock-step
            try:
                self._process_include_file(ifile, task, host_blocks)
            except ansible.errors.AnsibleError as e:
                for host in ifile._hosts:
                    self._tqm._failed_hosts[host.name] = True
                    self._iterator.mark_host_failed(host)
                    self._run_result |= self._tqm.RUN_FAILED_BREAK_PLAY
                LOG.error('%s', to_text(e), wrap_text=False)

        for host in host_blocks:
            self._iterator.add_tasks(host, host_blocks[host])
