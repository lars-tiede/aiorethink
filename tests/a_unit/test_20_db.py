import asyncio

import pytest
import rethinkdb as r

import aiorethink as ar


@pytest.mark.asyncio
async def test_simple_db_conn(db_conn):
    cn = await db_conn
    cn_alt = await db_conn.get()
    assert cn == cn_alt

    dbs = await r.db_list().run(cn)
    assert dbs != None

    await db_conn.close()


@pytest.mark.asyncio
async def test_cant_reconfigure_db_connection(db_conn):
    with pytest.raises(ar.AlreadyExistsError):
        ar.configure_db_connection(db = "foo")


@pytest.mark.asyncio
async def test_double_close(db_conn):
    cn = await db_conn
    await db_conn.close()
    await db_conn.close()


@pytest.mark.asyncio
async def test_double_close2(db_conn):
    cn = await db_conn
    await cn.close()
    await cn.close()


@pytest.mark.asyncio
async def test_double_close3(db_conn):
    cn = await db_conn
    await cn.close()
    await db_conn.close()


@pytest.mark.asyncio
async def test_double_close4(db_conn):
    cn = await db_conn
    await db_conn.close()
    await cn.close()


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_multithreading(db_conn, capsys):
    import threading

    cns = []
    cns_l = threading.Lock()
    ev = threading.Event()
    cn = await db_conn

    def thrd(i):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def do_stuff():
            # connect, run a query, disconnect, add (dead) connection object to list
            cn = await db_conn
            dbs = await r.db_list().run(cn)
            assert dbs != None
            await db_conn.close(False)
            assert not cn.is_open()
            with cns_l:
                cns.append(cn)
            # wait until main thread lets us continue
            await loop.run_in_executor(None, ev.wait)
            # make new connection and do some more
            cn = await db_conn
            dbs = await r.db_list().run(cn)
            await cn.close(False)
            assert not cn.is_open()

        # just run do_stuff, then terminate
        loop.run_until_complete(do_stuff())
        loop.run_until_complete(asyncio.gather( *asyncio.Task.all_tasks() ))
        assert len(asyncio.Task.all_tasks()) == 0
        loop.close()
        print("##TEST thread ran all the way through##")

    num_threads = 10
    threads = [ threading.Thread(target = thrd, args=(i,))
            for i in range(num_threads) ]
    [ t.start() for t in threads ]
    while True:
        await asyncio.sleep(0.1)
        with cns_l:
            if len(cns) == num_threads:
                break

    # make sure all cns are unique
    cns_set = set(cns)
    assert len(cns_set) == num_threads

    # let threads terminate
    ev.set()
    [ t.join() for t in threads ]

    # make sure all threads ran all the way through
    out, err = capsys.readouterr()
    import re
    matches = re.findall("##TEST thread ran all the way through##", out)
    assert len(matches) == num_threads


@pytest.fixture
def DocClasses(db_conn):
    class Doc1(ar.Document):
        pass

    class Doc2(ar.Document):
        @classmethod
        async def _create_table_extras(cls, conn = None):
            cn = conn or await db_conn
            await cls.cq().index_create('some_field').run(cn)

    class Doc3(ar.Document):
        @classmethod
        async def _reconfigure_table(cls, conn = None):
            cn = conn or await db_conn
            indexes = await cls.cq().index_list().run(cn)
            if len(indexes) == 0:
                await cls.cq().index_create('some_field').run(cn)

    return Doc1, Doc2, Doc3


@pytest.mark.asyncio
async def test_init_db_create_db(db_conn):
    cn = await db_conn
    dbs = await r.db_list().run(cn)

    assert "testing" not in dbs
    await ar.init_app_db()
    dbs = await r.db_list().run(cn)
    assert "testing" in dbs

    await r.db_drop("testing").run(cn)


@pytest.mark.asyncio
async def test_init_db_create_and_reconfigure_tables(DocClasses,
        db_conn, aiorethink_db_session):
    cn = await db_conn

    await ar.init_app_db()

    tables = await r.table_list().run(cn)
    assert len(tables) == len(DocClasses)
    for D in DocClasses:
        assert D._tablename in tables

    Doc1, Doc2, Doc3 = DocClasses

    indexes = await Doc1.cq().index_list().run(cn)
    assert len(indexes) == 0

    indexes = await Doc2.cq().index_list().run(cn)
    assert len(indexes) == 1

    indexes = await Doc3.cq().index_list().run(cn)
    assert len(indexes) == 0

    await ar.init_app_db()
    indexes = await Doc3.cq().index_list().run(cn)
    assert len(indexes) == 0

    await ar.init_app_db(reconfigure_db = True)
    indexes = await Doc3.cq().index_list().run(cn)
    assert len(indexes) == 1


@pytest.mark.asyncio
async def test_run_query(db_conn, aiorethink_db_session):
    cn = await db_conn

    q1 = r.db_list()
    q2 = r.db_list().run(cn)

    for q in [q1, q2]:
        res = await ar.db._run_query(q)
        assert "testing" in res

    with pytest.raises(TypeError):
        await ar.db._run_query("Hello")


@pytest.mark.asyncio
async def test_cursor_async_map(db_conn, aiorethink_db_session):
    from aiorethink.db import CursorAsyncMap
    cn = await db_conn

    await r.table_create("test").run(cn)
    for v in [1,2,3]:
        await r.table("test").insert({"v": v}).run(cn)

    cursor = await r.table("test").run(cn)
    mapper = lambda d: d["v"]

    vs = []
    async for v in CursorAsyncMap(cursor, mapper):
        assert type(v) == int
        vs.append(v)
    vs.sort()
    assert vs == [1,2,3]


    cursor = await r.table("test").run(cn)
    vs = await CursorAsyncMap(cursor, mapper).as_list()
    vs.sort()
    assert vs == [1,2,3]


@pytest.mark.asyncio
async def test_aiter_changes(event_loop, db_conn, aiorethink_db_session):
    cn = await db_conn

    await r.table_create("test").run(cn)

    async def track_table_changes(num_changes, vt):
        i = 0
        q = r.table("test").pluck("v").changes(include_states = True, include_initial = True)
        values = []
        async for obj, msg in await ar.aiter_changes(q, vt):
            i += 1
            assert obj == None or isinstance(list(obj.values())[0], set)
            if obj != None:
                values.append(list(obj.values())[0])
            assert "new_val" in msg
            if i >= num_changes:
                break
        return values

    table_tracker = event_loop.create_task(track_table_changes(3,
        ar.DictValueType(ar.StringValueType(), ar.SetValueType(ar.IntValueType()))
        ))

    await asyncio.sleep(0.5) # make reasonably sure that table_tracker is listening
    await r.table("test").insert({"id": 1, "v": [1,2,3]}).run(cn)
    await r.table("test").insert({"v": [4,5]}).run(cn)
    await r.table("test").get(1).delete().run(cn)
    await r.table("test").insert({"v": [6]}).run(cn)
    await r.table("test").insert({"v": [7]}).run(cn)

    done, pending = await asyncio.wait([table_tracker], timeout=5.0)
    assert table_tracker in done
    if table_tracker.exception() != None:
        table_tracker.print_stack()
    assert table_tracker.exception() == None
    assert table_tracker.result() == [{1,2,3}, {4,5}]
