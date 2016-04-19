import pytest
import rethinkdb as r

import aiorethink


###############################################################################
# general LazyField functionality
###############################################################################

@pytest.fixture
def SimpleLazyField():
    class SimpleLazyField(aiorethink.LazyField):

        class Value(aiorethink.LazyField.Value):
            async def _load(self, val_doc, conn = None):
                if val_doc != None:
                    return val_doc.swapcase()
                else:
                    return None

            def _convert_to_doc(self, val_cached):
                if val_cached != None:
                    return val_cached.swapcase()
                else:
                    return None

    return SimpleLazyField


@pytest.fixture
def DocSimpleLazyField(aiorethink_session, SimpleLazyField):
    class DocSimpleLazyField(aiorethink.Document):
        f1 = SimpleLazyField()
    return DocSimpleLazyField


def test_repr(DocSimpleLazyField):
    d = DocSimpleLazyField()
    assert "loaded=False" in repr(d.f1)


def test_set_and_get(DocSimpleLazyField):
    d = DocSimpleLazyField()

    assert d.f1.loaded == False
    with pytest.raises(aiorethink.NotLoadedError):
        v = d.f1.get()

    d.f1.set("Hello")
    assert d.f1.loaded == True
    assert d.f1.get() == "Hello"

    d.f1 = "World"
    assert d.f1.get() == "World"

    del d.f1
    assert d.f1.loaded == False
    with pytest.raises(aiorethink.NotLoadedError):
        v = d.f1.get()


@pytest.mark.asyncio
async def test_await(DocSimpleLazyField):
    d = DocSimpleLazyField()

    assert d.f1.loaded == False
    val = await d.f1
    assert d.f1.loaded == True
    assert val == None

    d.f1.set("Hello")
    val = await d.f1
    assert val == "Hello"


@pytest.mark.asyncio
async def test_from_doc(DocSimpleLazyField):
    s = { "f1": "hELLO" }
    d = DocSimpleLazyField.from_doc(s, False)

    assert d.f1.loaded == False
    v = await d.f1.load()
    assert v == "Hello"
    assert d.f1.loaded == True
    assert d.f1.get() == "Hello"

    s = {}
    d = DocSimpleLazyField.from_doc(s, False)

    assert d.f1.loaded == False
    v = await d.f1.load()
    assert v == None
    assert d.f1.loaded == True
    assert d.f1.get() == None


def test_to_doc(DocSimpleLazyField):
    d = DocSimpleLazyField()

    d.f1 = "hELLO"
    s = d.to_doc()
    assert s["f1"] == "Hello"

    d = DocSimpleLazyField()

    s = d.to_doc()
    assert s["f1"] == None


@pytest.mark.asyncio
async def test_validation(aiorethink_session, SimpleLazyField):
    def validate_all_lowercase(fld, val):
        if not val.islower():
            raise aiorethink.ValidationError("not lowercase")
        return val

    class Doc(aiorethink.Document):
        f1 = SimpleLazyField(extra_validators = (validate_all_lowercase,))

    d = Doc()
    d.f1 = "hello"
    assert d.validate_field("f1").get() == "hello"
    d.f1 = "HELLO"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f1")
    d.f1 = "hello"

    s = { "f1": "HELLO" }
    d = Doc.from_doc(s, False)
    assert d.validate_field("f1")
    await d.f1
    assert d.validate_field("f1")

    s = { "f1": "hello" }
    d = Doc.from_doc(s, False)
    assert d.validate_field("f1") # not loaded yet
    await d.f1
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f1")



@pytest.mark.asyncio
async def test_lazy_typed_field(aiorethink_db_session, db_conn):
    class MyStringField(aiorethink.StringField):
        def _convert_to_doc(self, obj, val):
            return val.upper()
        def _construct_from_doc(self, obj, val):
            return val.lower()

    class MyLazyStringField(aiorethink.LazyField, MyStringField):
        class Value(aiorethink.LazyField.Value):
            def _convert_to_doc(self, val_deconstructed):
                assert val_deconstructed.isupper()
                return val_deconstructed[-1::-1]
            async def _load(self, val_doc, conn = None):
                loaded_value = val_doc[-1::-1]
                assert loaded_value.isupper()
                return loaded_value

    class Doc(aiorethink.Document):
        f1 = MyLazyStringField(regex = "^[a-z]+$")
    await Doc._create_table()

    d = Doc()
    d.f1 = "abc"
    assert d.f1.get() == "abc"
    await d.save()
    my_id = d.pkey

    d = await Doc.load(my_id)
    await d.f1.load()
    assert d.f1.get() == "abc"

    cn = await db_conn
    v = await Doc.cq().get(my_id).get_field(Doc.f1.dbname).run(cn)
    assert v == "CBA"

    d.f1 = "INVALID"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f1")


###############################################################################
# ReferenceField
###############################################################################

@pytest.fixture
def RefFieldTestDocs(aiorethink_db_session, event_loop):
    class Doc1(aiorethink.Document):
        pass

    class Doc2(aiorethink.Document):
        f1 = aiorethink.ReferenceField(Doc1)

    arun = event_loop.run_until_complete
    arun(Doc1._create_table())
    arun(Doc2._create_table())

    return Doc1, Doc2


@pytest.mark.asyncio
async def test_reference_field(RefFieldTestDocs):
    Doc1, Doc2 = RefFieldTestDocs

    d1 = Doc1()
    await d1.save()

    d2 = Doc2()
    d2.f1 = d1
    await d2.save()
    d2id = d2.id

    d2 = await Doc2.load(d2id)
    d2_ref = await d2.f1
    assert d2_ref.__class__ == d1.__class__
    assert d2_ref.to_doc() == d1.to_doc()


@pytest.mark.asyncio
async def test_ref_to_unsaved_doc(RefFieldTestDocs):
    Doc1, Doc2 = RefFieldTestDocs

    d1 = Doc1()
    d2 = Doc2()

    d2.f1 = d1
    with pytest.raises(aiorethink.ValidationError):
        await d2.save()

    await d1.save()
    await d2.save()
    d2id = d2.id

    d2 = await Doc2.load(d2id)
    d2_ref = await d2.f1
    assert d2_ref.__class__ == d1.__class__
    assert d2_ref.to_doc() == d1.to_doc()
