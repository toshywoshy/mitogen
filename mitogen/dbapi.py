# Copyright 2018, David Wilson
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

import itertools

apilevel = '2.0'
threadsafety = 2
paramstyle = 'format'

_handler_id_ctr = itertools.count()
_cursor_id_ctr = itertools.count()
_handler_by_id = {}


#
# Exceptions.
# https://www.python.org/dev/peps/pep-0249/#exceptions
#

class DbapiError(StandardError):
    pass


class Warning(DbapiError):
    pass


class Error(DbapiError):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DataError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class InternalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


EXCEPTION_NAMES = (
    'Warning',
    'Error',
    'InterfaceError',
    'DatabaseError',
    'DataError',
    'OperationalError',
    'IntegrityError',
    'InternalError',
    'ProgrammingError',
    'NotSupportedError',
)


#
# Type Objects and Constructors.
# https://www.python.org/dev/peps/pep-0249/#type-objects-and-constructors
#

class Factory(object):
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        return '%s.%s%s' % (__name__, self.__class__.name, self.args)

    def dump(self):
        return {'$': self.__class__.name, '.': self.args}

    @classmethod
    def dump_seq(cls, seq):
        return tuple(obj.dump() if isinstance(obj, cls) else obj
                     for obj in seq)

    @classmethod
    def load_seq(cls, module, seq):
        return tuple(getattr(module, obj['$'])(*obj['.'])
                     if isinstance(obj, dict) else obj
                     for obj in seq)

class Date(Factory):
    pass

class Time(Factory):
    pass

class Timestamp(Factory):
    pass

class DateFromTicks(Factory):
    pass

class TimeFromTicks(Factory):
    pass

class TimestampFromTicks(Factory):
    pass

class Binary(Factory):
    pass

class STRING(Factory):
    pass

class BINARY(Factory):
    pass

class NUMBER(Factory):
    pass

class DATETIME(Factory):
    pass

class ROWID(Factory):
    pass


class CursorHandler(object):
    def __init__(self, conn_handler):
        self.conn_handler = conn_handler
        self.real_cursor = self.conn_handler.real_connection.cursor()

    def close(self):
        ret = self.real_connection.close()
        del _handler_by_id[self.id]
        return ret

    def commit(self):
        return self.real_connection.commit()

    def rollback(self):
        return self.real_connection.rollback()


class Cursor(object):
    def __init__(self, connection, id_):
        self.connection = connection
        self.id = id_

    def _invoke(self, *args, **kwargs):
        if not self.connection.open:
            raise DatabaseError('connection is closed')
        return self.context.call(_invoke, self.connection.id,
                                 'cursor_invoke', self.id, *args, **kwargs)

    def __del__(self):
        self.connection.context.call(
            _invoke,
            self.connection.id,
            'cursor_forget',
            self.id)

    @property
    def description(self):
        return self._invoke('description')

    @property
    def arraysize(self):
        return self._invoke('get_arraysize')

    @arraysize.setter
    def _set_arraysize(self, value):
        self._invoke('set_arraysize', value)

    @property
    def rowcount(self):
        return self._invoke('rowcount')

    def callproc(self, procname, *args, **kwargs):
        return self._invoke('callproc', *args, **kwargs)

    def close(self):
        return self._invoke('close')

    def execute(self, operation, params):
        return self._invoke('execute', Factory.dump_seq(params))

    def executemany(self, operation, seq_of_params):
        return self._invoke('executemany', operation,
                            map(Factory.dump_seq, seq_of_params))

    def fetchone(self):
        return self._invoke('fetchone')

    def fetchmany(self, size=None):
        return self._invoke('fetchmany', size)


class ConnectionHandler(object):
    def __init__(self, mod_name, args, kwargs):
        d = {}
        exec 'import %s as mod' % (mod_name,) in d
        mod = d['mod']

        self.exceptions = tuple(getattr(mod, k) for k in EXCEPTION_NAMES)
        self.real_connection = mod.connect(*args, **kwargs)
        self.cursor_by_id = {}

        self.id = next(_handler_id_ctr)
        _handler_by_id[self.id] = self

    def close(self):
        ret = self.real_connection.close()
        del _handler_by_id[self.id]
        return ret

    def commit(self):
        return self.real_connection.commit()

    def rollback(self):
        return self.real_connection.rollback()

    def cursor(self):
        id_ = next(_cursor_id_ctr)
        self.cursor_by_id[id_] = CursorHandler(self)
        return id_

    def cursor_invoke(self, cursor_id, method_name, *args, **kwargs):
        handler = self.cursor_by_id[cursor_id]
        method = getattr(handler, method_name)
        return method(*args, **kwargs)

    def _forget_cursor(self, cursor_id):
        del self.cursor_by_id[cursor_id]


class Connection(object):
    def __init__(self, context, conn_id):
        self.context = context
        self.id = conn_id
        self.open = True

    def _invoke(self, *args, **kwargs):
        if not self.open:
            raise DatabaseError('connection is closed')
        return self.context.call(_invoke, self.id, *args, **kwargs)

    def close(self):
        if self.open:
            self._invoke('close')
            self.open = False

    def commit(self):
        self._invoke('commit')

    def rollback(self):
        self._invoke('rollback')

    def cursor(self):
        handle = self._invoke('cursor')
        return Cursor(self, handle)


def _invoke(handler_id, method_name, *args, **kwargs):
    handler = _handler_by_id[handler_id]
    method = getattr(handler, method_name)
    return method(*args, **kwargs)


def _connect(mod_name, args, kwargs):
    handler = ConnectionHandler(mod_name, args, kwargs)
    return handler.id

def connect(context, module, *args, **kwargs):
    conn_id = context.call(_connect, module.__name__, args, kwargs)
    return Connection(context, conn_id)
