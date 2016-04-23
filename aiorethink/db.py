import threading
import asyncio
import weakref
from functools import partial
import inspect
import collections

import rethinkdb as r
r.set_loop_type("asyncio")

from .errors import IllegalAccessError, AlreadyExistsError
from .registry import registry

__all__ = [ "db_conn", "init_app_db", "configure_db_connection", "aiter_changes" ]


###############################################################################
# DB connections
###############################################################################

class _OneConnPerThreadPool:
    """Keeps track of one RethinkDB connection per thread.
    
    Get (or create) the current thread's connection with get() or just
    __await__. close() closes and discards the the current thread's connection
    so that a subsequent __await__ or get opens a new connection.
    """

    def __init__(self):
        self._tl = threading.local()
        self._connect_kwargs = None


    def configure_db_connection(self, **connect_kwargs):
        if self._connect_kwargs != None:
            raise AlreadyExistsError("Can not re-configure DB connection(s)")
        self._connect_kwargs = connect_kwargs


    def __await__(self):
        return self.get().__await__()


    async def get(self):
        """Gets or opens the thread's DB connection.
        """
        if self._connect_kwargs == None:
            raise IllegalAccessError("DB connection parameters not set yet")

        if not hasattr(self._tl, "conn"):
            self._tl.conn = await r.connect(**self._connect_kwargs)

        return self._tl.conn


    async def close(self, noreply_wait = True):
        """Closes the thread's DB connection.
        """
        if hasattr(self._tl, "conn"):
            if self._tl.conn.is_open():
                await self._tl.conn.close(noreply_wait)
            del self._tl.conn

db_conn = _OneConnPerThreadPool()


def configure_db_connection(db, **kwargs_for_rethink_connect):
    """Sets DB connection parameters. This function should be called exactly
    once, before init_app_db is called or db_conn is first used.
    """
    db_conn.configure_db_connection(db = db, **kwargs_for_rethink_connect)



###############################################################################
# DB setup (tables and such)
###############################################################################

async def init_app_db(reconfigure_db = False, conn = None):
    cn = conn or await db_conn

    # create DB if it doesn't exist
    our_db = db_conn._connect_kwargs["db"]
    dbs = await r.db_list().run(cn)
    if our_db not in dbs:
        await r.db_create(our_db).run(cn)

    # (re)configure DB tables
    for doc_class in registry.values():
        if not await doc_class.table_exists(cn):
            await doc_class._create_table(cn)
        elif reconfigure_db:
            await doc_class._reconfigure_table(cn)



###############################################################################
# DB query helpers
###############################################################################

async def _run_query(query, conn = None):
    """`run()`s query if caller hasn't already done so, then awaits and returns
    its result.

    If run() has already been called, then the query (strictly speaking, the
    awaitable) is just awaited. This gives the caller the opportunity to
    customize the run() call.

    If run() has not been called, then the query is run on the given connection
    (or the default connection). This is more convenient for the caller than
    the other version.
    """
    # run() it if caller didn't do that already
    if not inspect.isawaitable(query):
        if not isinstance(query, r.RqlQuery):
            raise TypeError("query is neither awaitable nor a RqlQuery")
        cn = conn or await db_conn
        query = query.run(cn)

    return await query



async def aiter_changes(query, value_type, conn = None):
    """Runs any changes() query, and from its result stream constructs "Python
    world" objects as determined by value_type (which may equal None when
    data is deleted from the DB).

    The function returns an asynchronous iterator (a ``ChangesAsyncMap``),
    which yields `(constructed python object, changefeed message)` tuples.
    Note that `constructed python object` might well be None.

    The `query` might or might not already have called `run()`, but it should
    not have been awaited on yet (check ``_run_query`` for details).
    """
    feed = await _run_query(query, conn)
    mapper = value_type.dbval_to_pyval
    return ChangesAsyncMap(feed, mapper)



###############################################################################
# Asynchronous iterators over cursors and changefeeds
###############################################################################

class CursorAsyncIterator(collections.abc.AsyncIterator):
    """Async iterator that iterates over a RethinkDB cursor until it's empty.
    """
    def __init__(self, cursor):
        self.cursor = cursor


    async def __aiter__(self):
        return self


    async def __anext__(self):
        try:
            return await self.cursor.next()
        except r.ReqlCursorEmpty:
            raise StopAsyncIteration


    async def as_list(self):
        """Turns the asynchronous iterator into a list by doing the iteration
        and collecting the resulting items into a list.
        """
        l = []
        async for item in self:
            l.append(item)
        return l



class CursorAsyncMap(CursorAsyncIterator):
    """Async iterator that iterates through a RethinkDB cursor, mapping each
    object coming out of the cursor to a supplied mapper function.
    
    Example: Document.from_cursor(cursor) returns a CursorAsyncMap that maps
    each object from the cursor to Document.from_doc().

    The ``as_list()`` coroutine creates a list out of the iterated items.
    """
    def __init__(self, cursor, mapper):
        """cursor is a RethinkDB cursor. mapper is a function accepting one
        parameter: whatever comes out of cursor.next().
        """
        super().__init__(cursor)
        self.mapper = mapper


    async def __anext__(self):
        item = await super().__anext__()
        mapped = self.mapper(item)
        return mapped



class ChangesAsyncMap(CursorAsyncIterator):
    """Async iterator that iterates over a RethinkDB changefeed, mapping each
    new_val coming in to a supplied mapper function (that typically makes some
    Python object out of it). On each iteration, a tuple (mapped object,
    changefeed message) is yielded. Note that the mapped object might well be
    None, for instance when documents are deleted from the DB.
    
    Changefeed messages that do not contain a `new_val` (status messages) are
    ignored.

    Example: ``Document.aiter_changes()`` returns a ChangesAsyncMap that maps
    each new_val (i.e., changed and inserted documents) to Document.from_doc().
    """
    def __init__(self, changefeed, mapper):
        """`changefeed` is a RethinkDB changes stream (technically, a RethinkDB
        cursor). `mapper` is a function accepting one parameter: a `new_val`
        from a changefeed message.
        """
        super().__init__(changefeed)
        self.mapper = mapper


    async def __anext__(self):
        # process and yield next message from changefeed that carries a
        # "new_val"
        while True:
            message = await super().__anext__()

            if "new_val" not in message:
                continue

            mapped = self.mapper(message["new_val"])
            return mapped, message


    async def as_list(self):
        """This is verboten on changefeeds as they have infinite length.
        """
        raise NotImplementedError("as_list makes no sense on changefeeds")
