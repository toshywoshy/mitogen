# Copyright 2017, David Wilson
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors
# may be used to endorse or promote products derived from this software without
# specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from __future__ import absolute_import
import errno
import logging
import multiprocessing
import os
import signal
import socket
import sys

import mitogen
import mitogen.core
import mitogen.debug
import mitogen.master
import mitogen.parent
import mitogen.service
import mitogen.unix
import mitogen.utils

import ansible_mitogen.logging
import ansible_mitogen.services

from mitogen.core import b


LOG = logging.getLogger(__name__)


class MuxProcess(object):
    """
    Implement a subprocess forked from the Ansible top-level, as a safe place
    to contain the Mitogen IO multiplexer thread, keeping its use of the
    logging package (and the logging package's heavy use of locks) far away
    from the clutches of os.fork(), which is used continuously by the
    multiprocessing package in the top-level process.

    The problem with running the multiplexer in that process is that should the
    multiplexer thread be in the process of emitting a log entry (and holding
    its lock) at the point of fork, in the child, the first attempt to log any
    log entry using the same handler will deadlock the child, as in the memory
    image the child received, the lock will always be marked held.

    See https://bugs.python.org/issue6721 for a thorough description of the
    class of problems this worker is intended to avoid.
    """

    #: In the top-level process, this references one end of a socketpair(),
    #: which the MuxProcess blocks reading from in order to determine when
    #: the master process dies. Once the read returns, the MuxProcess will
    #: begin shutting itself down.
    cls_worker_sock = None

    #: In the worker process, this references the other end of
    #: :py:attr:`cls_worker_sock`.
    cls_child_sock = None

    #: A copy of :data:`os.environ` at the time the multiplexer process was
    #: started. It's used by mitogen_local.py to find changes made to the
    #: top-level environment (e.g. vars plugins -- issue #297) that must be
    #: applied to locally executed commands and modules.
    cls_original_env = None

    #: In both processes, this a listof the temporary UNIX sockets used for
    #: forked WorkerProcesses to contact the forked mux processes.
    cls_listener_paths = None

    def __init__(self, path):
        #: Individual path of this process.
        self.listener_path = path

    def worker_main(self):
        """
        The main function of for the mux process: setup the Mitogen broker
        thread and ansible_mitogen services, then sleep waiting for the socket
        connected to the parent to be closed (indicating the parent has died).
        """
        self._setup_master()
        self._setup_services()

        # Let the parent know our listening socket is ready.
        mitogen.core.io_op(self.cls_child_sock.send, b('1'))
        self.cls_child_sock.send(b('1'))
        # Block until the socket is closed, which happens on parent exit.
        mitogen.core.io_op(self.cls_child_sock.recv, 1)

    def _setup_master(self):
        """
        Construct a Router, Broker, and mitogen.unix listener
        """
        self.router = mitogen.master.Router(max_message_size=4096 * 1048576)
        self.router.responder.whitelist_prefix('ansible')
        self.router.responder.whitelist_prefix('ansible_mitogen')
        mitogen.core.listen(self.router.broker, 'shutdown', self.on_broker_shutdown)
        mitogen.core.listen(self.router.broker, 'exit', self.on_broker_exit)
        self.listener = mitogen.unix.Listener(
            router=self.router,
            path=self.listener_path,
        )
        if 'MITOGEN_ROUTER_DEBUG' in os.environ:
            self.router.enable_debug()
        if 'MITOGEN_DUMP_THREAD_STACKS' in os.environ:
            mitogen.debug.dump_to_logger()

    def _setup_services(self):
        """
        Construct a ContextService and a thread to service requests for it
        arriving from worker processes.
        """
        self.pool = mitogen.service.Pool(
            router=self.router,
            services=[
                mitogen.service.FileService(router=self.router),
                mitogen.service.PushFileService(router=self.router),
                ansible_mitogen.services.ContextService(self.router),
                ansible_mitogen.services.ModuleDepService(self.router),
            ],
            size=int(os.environ.get('MITOGEN_POOL_SIZE', '16')),
        )
        LOG.debug('Service pool configured: size=%d', self.pool.size)

    def on_broker_shutdown(self):
        """
        Respond to broker shutdown by beginning service pool shutdown. Do not
        join on the pool yet, since that would block the broker thread which
        then cannot clean up pending handlers, which is required for the
        threads to exit gracefully.
        """
        self.pool.stop(join=False)
        try:
            os.unlink(self.listener.path)
        except OSError as e:
            # Prevent a shutdown race with the parent process.
            if e.args[0] != errno.ENOENT:
                raise

    def on_broker_exit(self):
        """
        Respond to the broker thread about to exit by sending SIGTERM to
        ourself. In future this should gracefully join the pool, but TERM is
        fine for now.
        """
        os.kill(os.getpid(), signal.SIGTERM)


def _start_child(path):
    pid = os.fork()
    if pid:
        # Wait for child to boot before continuing.
        mitogen.core.io_op(MuxProcess.cls_worker_sock.recv, 1)
        return

    MuxProcess.cls_worker_sock.close()
    MuxProcess.cls_worker_sock = None
    proc = MuxProcess(path)
    try:
        proc.worker_main()
    finally:
        sys.exit()


def start():
    """
    Arrange for the subprocess to be started, if it is not already running.

    The parent process picks a UNIX socket path the child will use prior to
    fork, creates a socketpair used essentially as a semaphore, then blocks
    waiting for the child to indicate the UNIX socket is ready for use.
    """
    if MuxProcess.cls_listener_paths is not None:
        return

    MuxProcess.cls_original_env = dict(os.environ)
    ansible_mitogen.logging.setup()

    MuxProcess.cls_listener_paths = [
        mitogen.unix.make_socket_path()
        for _ in range(multiprocessing.cpu_count())
    ]

    MuxProcess.cls_worker_sock, \
    MuxProcess.cls_child_sock = socket.socketpair()

    mitogen.core.set_cloexec(MuxProcess.cls_worker_sock.fileno())
    mitogen.core.set_cloexec(MuxProcess.cls_child_sock.fileno())

    for path in MuxProcess.cls_listener_paths:
        _start_child(path)

    MuxProcess.cls_child_sock.close()
    MuxProcess.cls_child_sock = None
