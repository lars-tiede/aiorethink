import threading
import asyncio
import weakref
from functools import partial

import rethinkdb as r
r.set_loop_type("asyncio")

from .errors import IllegalAccessError, AlreadyExistsError
from .registry import registry

__all__ = [ "db_conn", "init_app_db", "configure_db_connection" ]



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



class CursorAsyncMap:
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
        self.cursor = cursor
        self.mapper = mapper


    async def __aiter__(self):
        return self


    async def __anext__(self):
        try:
            item = await self.cursor.next()
            mapped = self.mapper(item)
            return mapped
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
