import asyncio

import rethinkdb as r
import pytest

import aiorethink as ar

@pytest.fixture
def EmptyDoc(aiorethink_session):
    class EmptyDoc(ar.Document):
        pass
    return EmptyDoc


def test_repr(EmptyDoc):
    d = EmptyDoc()
    r = repr(d)
    s = str(d)


###############################################################################
# id field and pkey alias to primary key field
###############################################################################

def test_base_doc_class_has_no_fields(aiorethink_session):
    assert not hasattr(ar.Document, "id")
    assert not hasattr(ar.Document, "pkey")


def test_emptydoc_has_id_attr(EmptyDoc):
    assert hasattr(EmptyDoc, "id") and isinstance(EmptyDoc.id, ar.Field)
    d = EmptyDoc()

    assert len(d) == 1 # 'id' attribute we got automatically
    assert d.len(ar.ALL) == 1
    assert d.len(ar.UNDECLARED_ONLY) == 0
    assert d.len(ar.DECLARED_ONLY) == 1
    assert d.id == None # didn't get one yet


def test_emptydoc_has_pkey_alias(EmptyDoc):
    assert hasattr(EmptyDoc, "pkey") and isinstance(EmptyDoc.pkey, ar.FieldAlias)


def test_pkey_aliases_id(EmptyDoc):
    d = EmptyDoc()
    assert d.id == d.pkey == None
    d.id = 1
    assert d.id == d.pkey == 1
    d.pkey = 2
    assert d.id == d.pkey == 2


def test_pkey_must_be_alias(aiorethink_session):
    with pytest.raises(ar.IllegalSpecError):
        class MyDoc(ar.Document):
            pkey = ar.Field()


def test_id_is_primary_key(aiorethink_session):
    class MyDoc(ar.Document):
        id = ar.Field(primary_key = True)

    d = MyDoc()
    assert d.pkey == d.id
    d.id = 1
    assert d.pkey == d.id


def test_id_but_no_primary_key(aiorethink_session):
    with pytest.raises(ar.IllegalSpecError):
        class MyDoc(ar.Document):
            id = ar.Field()


def test_two_primary_keys(aiorethink_session):
    with pytest.raises(ar.IllegalSpecError):
        class MyDoc(ar.Document):
            f1 = ar.Field(primary_key = True)
            f2 = ar.Field(primary_key = True)


def test_custom_primary_key(aiorethink_session):
    class MyDoc(ar.Document):
        f1 = ar.Field(primary_key = True)
    d = MyDoc()
    assert len(d) == 1 # has only the primary key attribute
    assert not hasattr(d, "id")
    assert "f1" in repr(d)

    d.f1 = 1
    assert d.pkey == d.f1


def test_copy_removes_pkey(MyTestDocs):
    for Doc in MyTestDocs:
        d = Doc()
        d.pkey = 1
        d.f2 = "hello"

        e = d.copy()
        assert d.pkey != None
        assert e.pkey == None
        assert e.f2 == d.f2 == "hello"


###############################################################################
# DB table stuff
###############################################################################

def test_doc_has_tablename(EmptyDoc):
    assert isinstance(EmptyDoc._tablename, str) and len(EmptyDoc._tablename) > 0


@pytest.fixture
def EmptyDocCustomTableName(aiorethink_session):
    class EmptyDocCustom(ar.Document):
        @classmethod
        def _get_tablename(cls):
            return "CustomTableName"
    return EmptyDocCustom


@pytest.mark.asyncio
async def test_table_does_not_exist(EmptyDoc, aiorethink_db_session):
    assert not await EmptyDoc.table_exists()


def test_custom_table_name(EmptyDocCustomTableName):
    assert EmptyDocCustomTableName._tablename == "CustomTableName"


@pytest.mark.asyncio
async def test_custom_table_does_not_exist(EmptyDocCustomTableName, aiorethink_db_session):
    assert not await EmptyDocCustomTableName.table_exists()


@pytest.mark.asyncio
async def test_create_table(EmptyDoc, db_conn, aiorethink_db_session):
    await EmptyDoc._create_table()
    assert await EmptyDoc.table_exists()

    cn = await db_conn
    assert EmptyDoc._tablename in await r.table_list().run(cn)

    indices = await r.table(EmptyDoc._tablename).index_list().run(cn)
    assert len(indices) == 0

    with pytest.raises(ar.AlreadyExistsError):
        await EmptyDoc._create_table()


@pytest.mark.asyncio
async def test_create_custom_table(EmptyDocCustomTableName, db_conn, aiorethink_db_session):
    await EmptyDocCustomTableName._create_table()
    assert await EmptyDocCustomTableName.table_exists()

    cn = await db_conn
    assert "CustomTableName" in await r.table_list().run(cn)


@pytest.mark.asyncio
async def test_custom_table_create_options(db_conn, aiorethink_db_session):
    class CustomDoc(ar.Document):
        _table_create_options = {"durability": "soft"}

    await CustomDoc._create_table()

    cn = await db_conn
    conf = await r.table(CustomDoc._tablename).config().run(cn)
    assert conf["durability"] == "soft"


@pytest.mark.asyncio
async def test_custom_pkey(db_conn, aiorethink_db_session):
    class CustomPkey(ar.Document):
        f1 = ar.Field(primary_key = True)

    await CustomPkey._create_table()

    cn = await db_conn
    conf = await r.table(CustomPkey._tablename).config().run(cn)
    assert conf["primary_key"] == "f1"


@pytest.mark.asyncio
async def test_secondary_index(db_conn, aiorethink_db_session):
    class SecIndex(ar.Document):
        f1 = ar.Field(indexed = True)

    await SecIndex._create_table()

    cn = await db_conn
    indices = await r.table(SecIndex._tablename).index_list().run(cn)
    assert len(indices) == 1


###############################################################################
# save / load / delete / q / queries
###############################################################################

def test_new_doc_not_stored_in_db(EmptyDoc):
    d = EmptyDoc()
    assert not d.stored_in_db


@pytest.mark.asyncio
async def test_save_empty_doc(EmptyDoc, aiorethink_db_session, db_conn):
    await EmptyDoc._create_table()
    d = EmptyDoc()
    assert d.id == None
    assert d.stored_in_db == False

    await d.save()

    assert d.id != None
    assert d.stored_in_db == True
    cn = await db_conn
    num_docs = await r.table(EmptyDoc._tablename).count().run(cn)
    assert num_docs == 1


@pytest.mark.asyncio
async def test_create_doc(EmptyDoc, aiorethink_db_session, db_conn):
    await EmptyDoc._create_table()
    cn = await db_conn
    d = await EmptyDoc.create(f1 = 1, hello = "blah")
    assert d.stored_in_db
    num_docs = await r.table(EmptyDoc._tablename).count().run(cn)
    assert num_docs == 1


@pytest.fixture
def MyTestDocs(aiorethink_db_session, event_loop, db_conn):
    class Doc(ar.Document):
        f1 = ar.Field()
        f2 = ar.Field()
    class DocCustomPkey(ar.Document):
        f1 = ar.Field(primary_key = True)
        f2 = ar.Field()
    arun = event_loop.run_until_complete
    cn = arun(db_conn.get())
    arun(Doc._create_table())
    arun(DocCustomPkey._create_table())
    return Doc, DocCustomPkey


@pytest.mark.asyncio
async def test_save_custom_pkey_doc(MyTestDocs):
    _, DocCustomPkey = MyTestDocs

    d = DocCustomPkey()
    d.f1 = 1
    await d.save()
    assert d.f1 == 1
    assert d.stored_in_db

    d = DocCustomPkey()
    assert d.f1 == None
    await d.save()
    assert d.f1 != None
    assert d.stored_in_db


@pytest.mark.asyncio
async def test_q(MyTestDocs, db_conn):
    for Doc in MyTestDocs:
        d = Doc()
        d.f2 = 1

        await d.save()

        cn = await db_conn
        val = await d.q().get_field("f2").run(cn)
        assert val == 1


@pytest.mark.asyncio
async def test_update_doc(MyTestDocs, db_conn):
    for Doc in MyTestDocs:
        d = Doc()
        d.f2 = 1
        d["f3"] = 1
        await d.save()

        cn = await db_conn
        async def get_val(name):
            return await d.q().get_field(name).run(cn)
        f2 = await get_val("f2")
        assert f2 == 1
        f3 = await get_val("f3")
        assert f3 == 1

        d.f2 = 2
        d["f3"] = 2
        d["f4"] = 2
        await d.save()

        for f in ["f2", "f3", "f4"]:
            val = await get_val(f)
            assert val == 2

        await d.save()


@pytest.mark.asyncio
async def test_delete_doc(MyTestDocs, db_conn):
    cn = await db_conn
    for Doc in MyTestDocs:
        d = Doc()
        num_docs = await Doc.cq().count().run(cn)

        await d.save()

        num_docs_now = await Doc.cq().count().run(cn)
        assert num_docs_now == num_docs + 1

        await d.delete()
        assert not d.stored_in_db

        num_docs_now = await Doc.cq().count().run(cn)
        assert num_docs_now == num_docs


@pytest.mark.asyncio
async def test_load_doc(MyTestDocs, db_conn):
    cn = await db_conn
    for Doc in MyTestDocs:
        d = Doc(f2 = 1, f3 = 1)
        await d.save()

        l = await Doc.load(d["pkey"])
        assert l.stored_in_db
        for k in l:
            assert l[k] == d[k]

        with pytest.raises(ar.NotFoundError):
            l = await Doc.load("hello")


@pytest.mark.asyncio
async def test_from_cursor(EmptyDoc, db_conn, aiorethink_db_session):
    cn = await db_conn
    await EmptyDoc._create_table()

    for v in [1,2,3]:
        await EmptyDoc.create(v = v)

    c = await EmptyDoc.cq().run(cn)
    ds = await EmptyDoc.from_cursor(c).as_list()

    assert len(ds) == 3
    vs = [ d["v"] for d in ds ]
    vs.sort()
    assert vs == [1,2,3]


@pytest.mark.asyncio
async def test_from_query(EmptyDoc, db_conn, aiorethink_db_session):
    cn = await db_conn
    await EmptyDoc._create_table()

    for v in [1,2,3]:
        await EmptyDoc.create(v = v)

    q = EmptyDoc.cq()
    c = await EmptyDoc.from_query(q)
    assert isinstance(c, ar.db.CursorAsyncMap)

    q = EmptyDoc.cq().nth(0)
    d1 = await EmptyDoc.from_query(q)
    assert isinstance(d1, EmptyDoc)
    d2 = await EmptyDoc.from_query(q.run(cn))
    assert isinstance(d2, EmptyDoc)
    assert d1.pkey == d2.pkey

    q = EmptyDoc.cq().get("does not exist")
    assert await EmptyDoc.from_query(q) == None

    with pytest.raises(TypeError):
        await EmptyDoc.from_query(r)


###############################################################################
# Subclassing
###############################################################################

@pytest.mark.asyncio
async def test_subclass_trivial(aiorethink_db_session, db_conn):
    class GeneralDoc(ar.Document):
        pass
    class SpecializedDoc(GeneralDoc):
        pass

    await GeneralDoc._create_table()
    assert await GeneralDoc.table_exists()
    assert not await SpecializedDoc.table_exists()
    await SpecializedDoc._create_table()
    assert await SpecializedDoc.table_exists()

    cn = await db_conn
    assert GeneralDoc._tablename in await r.table_list().run(cn)
    assert SpecializedDoc._tablename in await r.table_list().run(cn)
    assert GeneralDoc._tablename != SpecializedDoc._tablename


@pytest.mark.asyncio
async def test_subclass_field_inheritance_sanity(aiorethink_db_session, db_conn):
    class GeneralDoc(ar.Document):
        f1 = ar.Field()
    class SpecializedDoc(GeneralDoc):
        f2 = ar.Field()

    dg = GeneralDoc()
    ds = SpecializedDoc()

    assert len(dg) == 2
    assert len(ds) == 3
    assert "f2" in ds
    assert "f2" not in dg
    assert "f1" in dg
    assert "f1" in ds


###############################################################################
# changefeeds
###############################################################################

@pytest.mark.asyncio
async def test_table_changefeed(EmptyDoc, db_conn, aiorethink_db_session, event_loop):
    cn = await db_conn
    await EmptyDoc._create_table()

    async def track_table_changes(num_changes):
        i = 0
        vals = []
        async for doc, change_msg in await EmptyDoc.aiter_table_changes():
            i += 1
            assert isinstance(doc, EmptyDoc)
            assert "new_val" in change_msg
            vals.append(doc["f1"])
            if i >= num_changes:
                break
        return vals

    table_tracker = event_loop.create_task(track_table_changes(3))
    await asyncio.sleep(0.5)
    for i in range(3):
        d = EmptyDoc(f1 = i)
        await d.save()
        await asyncio.sleep(0.2)
    done, pending = await asyncio.wait([table_tracker], timeout=1.0)
    
    assert table_tracker in done
    assert table_tracker.exception() == None
    assert table_tracker.result() == [0,1,2]


@pytest.mark.asyncio
async def test_doc_changefeed(EmptyDoc, db_conn, aiorethink_db_session, event_loop):
    cn = await db_conn
    await EmptyDoc._create_table()

    async def track_doc_changes(doc, num_changes):
        i = 0
        vals = []
        async for d, changed_fields, msg in await doc.aiter_changes():
            i += 1
            assert id(d) == id(doc)
            assert len(changed_fields) == 1
            assert "f1" in changed_fields
            assert "new_val" in msg
            vals.append(d["f1"])
            if i >= num_changes:
                break
        return vals

    d = EmptyDoc()
    await d.save()
    d2 = await EmptyDoc.load(d.pkey)

    doc_tracker = event_loop.create_task(track_doc_changes(d, 3))
    for i in range(3):
        await asyncio.sleep(0.2)
        d2["f1"] = i
        await d2.save()
    done, pending = await asyncio.wait([doc_tracker], timeout=1.0)

    assert doc_tracker in done
    if doc_tracker.exception() != None:
        doc_tracker.print_stack()
    assert doc_tracker.exception() == None
    assert doc_tracker.result() == [0,1,2]
