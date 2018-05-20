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
import ansible.plugins.loader
import ansible.plugins.strategy
import ansible.template
import ansible_mitogen.controller.includes

from ansible.executor.play_iterator import PlayIterator
from ansible.module_utils._text import to_text


LOG = logging.getLogger(__name__)


def _action_bypasses_host_loop(action_name):
    """
    Return :data:`True` if the action plug-in corresponding to
    `action_name` should execute only for the first host.
    """
    try:
        action = ansible.plugins.loader.action_loader.get(
            action_name,
            class_only=True,
        )
        return getattr(action, 'BYPASS_HOST_LOOP', False)
    except KeyError:
        # Action plugin missing (e.g. normal module execution).
        return False


def _is_duplicate_task(host, task):
    """
    Avoid repeat execution of a role unless it is requested.
    """
    return (
        task._role and
        task._role.has_run(host) and
        not getattr(task._role._metadata, 'allow_duplicates', False)
    )


def _clone(obj, **kwargs):
    """
    Return a copy of `obj` without calling its constructor, after updating its
    attributes with `kwargs`.
    """
    new = type(obj).__new__(type(obj))
    vars(new).update(vars(obj))
    vars(new).update(kwargs)
    return new


class StrategyModule(ansible.plugins.strategy.StrategyBase):
    """
    The linear strategy is simple - get the next task and queue it for all
    hosts, then wait for the queue to drain before moving on to the next task
    """
    NAME_BY_ITER_STATE = {
        PlayIterator.ITERATING_SETUP: 'ITERATING_SETUP',
        PlayIterator.ITERATING_TASKS: 'ITERATING_TASKS',
        PlayIterator.ITERATING_RESCUE: 'ITERATING_RESCUE',
        PlayIterator.ITERATING_ALWAYS: 'ITERATING_ALWAYS',
        PlayIterator.ITERATING_COMPLETE: 'ITERATING_COMPLETE'
    }

    #: Play progresses through each state in order, with no host proceeding to
    #: the next state until all hosts are ready.
    ITERATOR_PHASES = (
        PlayIterator.ITERATING_SETUP,
        PlayIterator.ITERATING_TASKS,
        PlayIterator.ITERATING_RESCUE,
        PlayIterator.ITERATING_ALWAYS,
    )

    DONT_FAIL_STATES = frozenset([
        PlayIterator.ITERATING_RESCUE,
        PlayIterator.ITERATING_ALWAYS
    ])

    def construct(self, iterator, play_context):
        """
        Finalize construction using parameters passed via :meth:`run`.
        """
        self._iterator = iterator
        self._play_context = play_context
        self._templar = ansible.template.Templar(loader=self._tqm.get_loader())
        self._run_result = self._tqm.RUN_OK
        self._includes = ansible_mitogen.controller.includes.Parser(
            tqm=self._tqm,
            play_context=self._play_context,
            var_manager=self._variable_manager,
            iterator=self._iterator,
        )

    def _peek_task(self, host):
        """
        Sanitized interface to PlayIterator.get_next_task_for_host().
        """
        return self._iterator.get_next_task_for_host(host, peek=True)

    def get_hosts_left(self):
        """
        Sanitized interface to StrategyBase.get_hosts_left().
        """
        return super(StrategyModule, self).get_hosts_left(self._iterator)

    def _execute_meta(self, task, host):
        """
        Sanitized interface to StrategyBase._execute_meta().
        """
        return super(StrategyModule, self)._execute_meta(
            task,
            self._play_context,
            self._iterator,
            host,
        )

    def _process_pending_results(self, max_passes=None):
        """
        Sanitized interface to StrategyBase._process_pending_results().
        """
        return super(StrategyModule, self)._process_pending_results(
            self._iterator,
            max_passes=max_passes,
        )

    def _advance_selected_hosts(self, cur_block, state):
        """
        Return the task for all hosts in the requested state, otherwise they
        get a noop dummy task. This also advances the state of the host, since
        the given states are determined while using peek=True.
        """
        LOG.debug("advancing hosts in %r", self.NAME_BY_ITER_STATE[state])

        tasks = []
        hosts = []
        for host in self.get_hosts_left():
            hstate, task = self._peek_task(host)
            if task is None:
                continue
            if hstate.run_state == state and hstate.cur_block == cur_block:
                self._iterator.get_next_task_for_host(host)  # advance.
                hosts.append(host)
                tasks.append(task)
            else:
                LOG.error('NOOP!')

        assert all(tasks[0] == t for t in tasks)
        return tasks[0], hosts

    def _get_next_task_lockstep(self):
        """
        Return a tuple of the next :class:`ansible.playbook.task.Task` to
        execute and a list of :class:`ansible.playbook.inventory.Host` to
        execute it on.
        """
        host_state_task = [
            (host, (hstate, task))
            for host, (hstate, task) in (
                (host, self._peek_task(host))
                for host in self.get_hosts_left()
            )
            if task
        ]

        if host_state_task:
            cur_block = min(
                hstate.cur_block
                for host, (hstate, task) in host_state_task
                if hstate.run_state != PlayIterator.ITERATING_COMPLETE
            )
            for state in self.ITERATOR_PHASES:
                if any(hstate.cur_block <= cur_block and
                       hstate.run_state == state
                       for host, (hstate, task) in host_state_task):
                    return self._advance_selected_hosts(cur_block, state)

        LOG.debug("all hosts are done, returning None for all hosts")
        return None, []

    def _is_any_errors_fatal_task(self, task):
        """
        Return :data:`True` if `task` was marked with ``any_errors_fatal:
        true``, and neither it nor its parent context had ``ignore_errors:
        false``.
        """
        return (not task.ignore_errors) and (
            task.any_errors_fatal or
            self._is_run_once_task(task)
        )

    def _check_any_errors_fatal(self, task, failed, unreachable):
        """
        If any_errors_fatal is :data:`True` and an error occurred, mark all
        hosts failed.
        """
        LOG.debug("_check_any_errors_fatal()")
        if (failed or unreachable) and self._is_any_errors_fatal_task(task):
            for host in self.get_hosts_left():
                hstate, task = self._peek_task(host)
                if hstate.run_state not in self.DONT_FAIL_STATES or \
                   hstate.run_state == self._iterator.ITERATING_RESCUE and \
                   hstate.fail_state & self._iterator.FAILED_RESCUE != 0:
                    self._tqm._failed_hosts[host.name] = True
        LOG.debug("done checking for any_errors_fatal")

    def _check_max_fail_percentage(self, results, failed, unreachable):
        LOG.debug("_check_max_fail_percentage()")
        if results and self._iterator._play.max_fail_percentage is not None:
            percentage = self._iterator._play.max_fail_percentage / 100.0
            failed_count = len(self._tqm._failed_hosts)
            if (failed_count / self._iterator.batch_size) > percentage:
                for host in self.get_hosts_left():
                    # don't double-mark hosts, or the iterator will potentially
                    # fail them out of the rescue/always states
                    if host.name not in failed:
                        self._tqm._failed_hosts[host.name] = True
                        self._iterator.mark_host_failed(host)
                self._tqm.send_callback('v2_playbook_on_no_hosts_remaining')
                self._run_result |= self._tqm.RUN_FAILED_BREAK_PLAY
            LOG.debug('(%s failed / %s total )> %s max fail' % (
                len(self._tqm._failed_hosts),
                self._iterator.batch_size,
                percentage
            ))

    def _check_failures(self, task, results):
        failed = []
        unreachable = []
        for res in results:
            if res.is_failed() and self._iterator.is_failed(res._host):
                failed.append(res._host.name)
            elif res.is_unreachable():
                unreachable.append(res._host.name)

        self._check_any_errors_fatal(task, failed, unreachable)
        self._check_max_fail_percentage(results, failed, unreachable)

    def _drain_results(self, task, results):
        LOG.debug("done queuing tasks, waiting for results to drain")
        results.extend(self._wait_on_pending_results(self._iterator))
        self.update_active_connections(results)
        self._check_failures(task, results)

    def _is_run_once_task(self, task):
        """
        Return :data:`True` if `task` is a meta task, uses an action plug-in
        that sets :attr:`BYPASS_HOST_LOOP`, or specifies ``run_once``.
        """
        return (task.action == 'meta' or
                _action_bypasses_host_loop(task.action) or
                self._templar.template(task.run_once))

    def _send_task_start_callback(self, task, task_vars):
        """
        For the first host of the first CPU, ..
        """
        self._templar.set_available_variables(task_vars)
        name = self._templar.template(task.name, fail_on_undefined=False)
        new_task = _clone(task, name=to_text(name, nonstring='empty'))
        self._tqm.send_callback('v2_playbook_on_task_start',
                                new_task, is_conditional=False)

    def _start_regular_task(self, host, task, cpu, host_index):
        task_vars = self._variable_manager.get_vars(
            play=self._iterator._play,
            host=host,
            task=task,
        )
        self.add_tqm_variables(task_vars, play=self._iterator._play)
        self._templar.set_available_variables(task_vars)
        if cpu == 0 and host_index == 0:
            self._send_task_start_callback(task, task_vars)
        self._queue_task(host, task, task_vars, self._play_context)

    def _run_next_task(self):
        task, hosts = self._get_next_task_lockstep()
        if task is None:
            return False

        results = []
        for host_index, host in enumerate(hosts):
            if _is_duplicate_task(host, task):
                LOG.debug("'%s' skipped because role has already run", task)
                continue

            if task.action == 'meta':
                results.extend(self._execute_meta(task, host))
            else:
                self._start_regular_task(host, task, 0, host_index)

            if self._is_run_once_task(task):
                break

            max_passes = max(1, len(self._tqm._workers) // 10)
            results.extend(self._process_pending_results(max_passes))
            try:
                self._includes.parse(task, results, self.get_hosts_left())
            except ansible_mitogen.includes.Error as e:
                LOG.error('include processing failed: %s', e)
                self._run_result = _self._tqm.RUN_ERROR

        self._drain_results(task, results)
        return True

    def _has_no_hosts_remaining(self):
        return (
            self._run_result != self._tqm.RUN_OK and
            len(self._tqm._failed_hosts) >= len(self.get_hosts_left())
        )

    def run(self, iterator, play_context):
        """
        See StrategyBase.run().
        """
        self.construct(iterator, play_context)
        while not self._tqm._terminated:
            try:
                if not self._run_next_task():
                    break
            except (IOError, EOFError):
                LOG.error('Most likely an abort, return failed')
                return self._tqm.RUN_UNKNOWN_ERROR

            if self._has_no_hosts_remaining():
                self._tqm.send_callback('v2_playbook_on_no_hosts_remaining')
                break

        return super(StrategyModule, self).run(
            iterator,
            play_context,
            self._run_result,
        )
