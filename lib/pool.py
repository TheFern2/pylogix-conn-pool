"""Connection pooling for psycopg2

This module implements thread-safe (and not) connection pools.
pylogix/pool.py - pooling code for pylogix

Copyright (C) 2023 Fernando B  <fernandobe+git@protonmail.com>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pylogix import PLC

from lib.plc_conn import PlcConn


class PoolError(Exception):
    pass


class AbstractConnectionPool:
    """Generic key-based pooling code."""

    def __init__(self, minconn, maxconn, *args, **kwargs):
        """Initialize the connection pool.

        New 'minconn' connections are created immediately calling 'connfunc'
        with given parameters. The connection pool will support a maximum of
        about 'maxconn' connections.
        """
        self.minconn = int(minconn)
        self.maxconn = int(maxconn)
        self.closed = False

        self._args = args
        self._kwargs = kwargs

        self._pool = []
        self._used = {}
        self._rused = {}  # id(conn) -> key map
        self._keys = 0

        for i in range(self.minconn):
            self._connect()

    def _connect(self, key=None):
        """Create a new connection and assign it to 'key' if not None."""
        # conn = PLC(*self._args, **self._kwargs)
        conn = PlcConn(*self._args, **self._kwargs)
        if key is not None:
            self._used[key] = conn
            self._rused[id(conn)] = key
        else:
            self._pool.append(conn)
        return conn

    def _getkey(self):
        """Return a new unique key."""
        self._keys += 1
        return self._keys

    def _getconn(self, key=None):
        """Get a free connection and assign it to 'key' if not None."""
        if self.closed:
            raise PoolError("connection pool is closed")
        if key is None:
            key = self._getkey()

        if key in self._used:
            return self._used[key]

        if self._pool:
            self._used[key] = conn = self._pool.pop()
            self._rused[id(conn)] = key
            print(f"conn key {key}, pool len {len(self._pool)}")
            print(self._used)
            conn.keepalive()
            return conn
        else:
            if len(self._used) == self.maxconn:
                raise PoolError("connection pool exhausted")
            return self._connect(key)

    def _putconn(self, conn: PlcConn, key=None, close=False):
        """Put away a connection."""
        if self.closed:
            raise PoolError("connection pool is closed")

        if key is None:
            key = self._rused.get(id(conn))
            if key is None:
                raise PoolError("trying to put unkeyed connection")

        if len(self._pool) < self.minconn and not close:
            # Return the connection into a consistent state before putting
            # it back into the pool
            # TODO fix this with custom PLC class
            # if conn.status() != "Successful":
            # if not conn.closed:
            # if not conn.conn.SocketConnected:
            #     status = conn.info.transaction_status
            #     if status == 2:
            #         # server connection lost
            #         conn.close()
            #     elif status != 3:
            #         # connection in error or in transaction
            #         conn.rollback()
            #         self._pool.append(conn)
            #     else:
                    # regular idle connection
            print(f"Put conn away {id(conn)}")
            self._pool.append(conn)
            # If the connection is closed, we just discard it.
        else:
            conn.Close()

        # here we check for the presence of key because it can happen that a
        # thread tries to put back a connection after a call to close
        if not self.closed or key in self._used:
            del self._used[key]
            del self._rused[id(conn)]

    def _closeall(self):
        """Close all connections.

        Note that this can lead to some code fail badly when trying to use
        an already closed connection. If you call .closeall() make sure
        your code can deal with it.
        """
        if self.closed:
            raise PoolError("connection pool is closed")
        for conn in self._pool + list(self._used.values()):
            try:
                # conn.close()
                conn.Close()
            except Exception:
                pass
        self.closed = True


class SimpleConnectionPool(AbstractConnectionPool):
    """A connection pool that can't be shared across different threads."""

    getconn = AbstractConnectionPool._getconn
    putconn = AbstractConnectionPool._putconn
    closeall = AbstractConnectionPool._closeall


class ThreadedConnectionPool(AbstractConnectionPool):
    """A connection pool that works with the threading module."""

    def __init__(self, minconn, maxconn, *args, **kwargs):
        """Initialize the threading lock."""
        import threading
        AbstractConnectionPool.__init__(
            self, minconn, maxconn, *args, **kwargs)
        self._lock = threading.Lock()

    def getconn(self, key=None):
        """Get a free connection and assign it to 'key' if not None."""
        self._lock.acquire()
        try:
            return self._getconn(key)
        finally:
            self._lock.release()

    def putconn(self, conn=None, key=None, close=False):
        """Put away an unused connection."""
        self._lock.acquire()
        try:
            self._putconn(conn, key, close)
        finally:
            self._lock.release()

    def closeall(self):
        """Close all connections (even the one currently in use.)"""
        self._lock.acquire()
        try:
            self._closeall()
        finally:
            self._lock.release()
