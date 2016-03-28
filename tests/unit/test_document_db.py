import pytest

import rethinkdb as r

import aiorethink
from aiorethink import db_conn


###############################################################################
# table creation
###############################################################################

@pytest.fixture
def EmptyDoc(aiorethink_db_session):
    class EmptyDoc(aiorethink.Document):
        pass
    return EmptyDoc

@pytest.fixture
def EmptyDocCustomTableName(aiorethink_db_session):
    class EmptyDocCustom(aiorethink.Document):
        _tablename = "CustomTableName"
    return EmptyDocCustom


@pytest.mark.asyncio
async def test_table_does_not_exist(EmptyDoc):
    assert not await EmptyDoc.table_exists()


@pytest.mark.asyncio
async def test_custom_table_does_not_exist(EmptyDocCustomTableName):
    assert not await EmptyDocCustomTableName.table_exists()
    assert EmptyDocCustomTableName._tablename == "CustomTableName"


@pytest.mark.asyncio
async def test_create_table(EmptyDoc):
    await EmptyDoc._create_table()
    assert await EmptyDoc.table_exists()

    cn = await db_conn
    assert EmptyDoc._tablename in await r.table_list().run(cn)

    indices = await r.table(EmptyDoc._tablename).index_list().run(cn)
    assert len(indices) == 0

    with pytest.raises(aiorethink.AlreadyExistsError):
        await EmptyDoc._create_table()


@pytest.mark.asyncio
async def test_create_custom_table(EmptyDocCustomTableName):
    await EmptyDocCustomTableName._create_table()
    assert await EmptyDocCustomTableName.table_exists()

    cn = await db_conn
    assert "CustomTableName" in await r.table_list().run(cn)


@pytest.mark.asyncio
async def test_custom_table_create_options(aiorethink_db_session):
    class CustomDoc(aiorethink.Document):
        _table_create_options = {"durability": "soft"}

    await CustomDoc._create_table()

    cn = await db_conn
    conf = await r.table(CustomDoc._tablename).config().run(cn)
    assert conf["durability"] == "soft"


@pytest.mark.asyncio
async def test_custom_pkey(aiorethink_db_session):
    class CustomPkey(aiorethink.Document):
        f1 = aiorethink.Field(primary_key = True)

    await CustomPkey._create_table()

    cn = await db_conn
    conf = await r.table(CustomPkey._tablename).config().run(cn)
    assert conf["primary_key"] == "f1"


@pytest.mark.asyncio
async def test_secondary_index(aiorethink_db_session):
    class SecIndex(aiorethink.Document):
        f1 = aiorethink.Field(indexed = True)

    await SecIndex._create_table()

    cn = await db_conn
    indices = await r.table(SecIndex._tablename).index_list().run(cn)
    assert len(indices) == 1


###############################################################################
# save / load / delete
###############################################################################

@pytest.mark.asyncio
async def test_save_empty_doc(EmptyDoc):
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
async def test_create_doc(EmptyDoc):
    await EmptyDoc._create_table()
    cn = await db_conn
    d = await EmptyDoc.create(f1 = 1, hello = "blah")
    assert d.stored_in_db
    num_docs = await r.table(EmptyDoc._tablename).count().run(cn)
    assert num_docs == 1


@pytest.fixture
def MyTestDocs(aiorethink_db_session, event_loop):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field()
        f2 = aiorethink.Field()
    class DocCustomPkey(aiorethink.Document):
        f1 = aiorethink.Field(primary_key = True)
        f2 = aiorethink.Field()
    arun = event_loop.run_until_complete
    cn = arun(db_conn.get())
    arun(Doc._create_table())
    arun(DocCustomPkey._create_table())
    return Doc, DocCustomPkey


@pytest.mark.asyncio
async def test_q(MyTestDocs):
    for Doc in MyTestDocs:
        d = Doc()
        d.f2 = 1

        await d.save()

        cn = await db_conn
        val = await d.q().get_field("f2").run(cn)
        assert val == 1


@pytest.mark.asyncio
async def test_update_doc(MyTestDocs):
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
async def test_delete_doc(MyTestDocs):
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
async def test_load_doc(MyTestDocs):
    cn = await db_conn
    for Doc in MyTestDocs:
        d = Doc(f2 = 1, f3 = 1)
        await d.save()

        l = await Doc.load(d["pkey"])
        assert l.stored_in_db
        for k in l:
            assert l[k] == d[k]

        with pytest.raises(aiorethink.NotFoundError):
            l = await Doc.load("hello")


@pytest.mark.asyncio
async def test_from_cursor(EmptyDoc):
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
