import asyncio

import pytest
import rethinkdb as r

import aiorethink as ar


###############################################################################
# Field and FieldAlias in isolation (i.e. not as class attribute in a
# FieldContainer)
###############################################################################

def test_basic_field():
    f = ar.Field()

    assert isinstance(f.val_type, ar.AnyValueType)

    assert f.name == None
    assert f.dbname == None # this is never the case when the field is inside a FieldContainer
    f.name = "f" # because this is done by FieldContainer
    assert f.name == "f"
    assert f.dbname == "f"

    assert not f.indexed
    assert not f.required
    assert not f.primary_key
    assert f.default == None

    assert f.validate(None) == f
    assert f.validate(1) == f
    assert f.validate("hello") == f


def test_field_with_type():
    f = ar.Field(ar.IntValueType())

    assert f.validate(1) == f
    with pytest.raises(ar.ValidationError):
        f.validate("hello")


def test_field_with_type_and_default():
    with pytest.raises(ar.IllegalSpecError):
        f = ar.Field(ar.IntValueType(), default = "hello")

    f = ar.Field(ar.StringValueType(), default = "hello")


def test_field_properties():
    f = ar.Field(default = "hello")
    assert f.default == "hello"

    f = ar.Field(indexed = True)
    assert f.indexed == True

    f = ar.Field(required = True)
    with pytest.raises(ar.ValidationError):
        f.validate(None)
    assert f.validate(10) == f

    f = ar.Field(primary_key = True)
    assert f.primary_key == True

    with pytest.raises(ar.IllegalSpecError):
        f = ar.Field(indexed = True, primary_key = True)

    f = ar.Field(name = "dbf")
    assert f.dbname == "dbf"
    f.name = "f" # this is what FieldContainer does
    assert f.name == "f"
    assert f.dbname == "dbf"


def test_repr():
    f = ar.Field()
    assert "dbname" in repr(f)


def test_fieldalias():
    f = ar.Field(ar.IntValueType())
    f.name = "f"
    fa = ar.FieldAlias(f)

    for attr in ["name", "dbname", "indexed", "required", "default", "validate"]:
        assert getattr(fa, attr) == getattr(f, attr)

    assert "fld_name=f" in repr(fa)


###############################################################################
# Field and FieldAlias inside FieldContainer
###############################################################################

def test_simple_field_inside_container():
    class FC(ar.FieldContainer):
        f = ar.Field()
        fa = ar.FieldAlias(f)

    assert FC.f.name == "f"
    assert FC.fa.name == "f"


def test_simple_get_set_delete():
    class FC(ar.FieldContainer):
        f1 = ar.Field()
        f1a = ar.FieldAlias(f1)
    fc = FC()

    assert fc.f1 == None
    assert fc.f1a == None
    fc.f1 = 1
    assert fc.f1 == 1
    assert fc.f1a == 1
    fc.f1a = 2
    assert fc.f1 == 2
    assert fc.f1a == 2
    del fc.f1
    assert fc.f1 == None
    assert fc.f1a == None
    del fc.f1a
    assert fc.f1 == None
    assert fc.f1a == None
    fc.f1a = 1
    assert fc.f1 == 1
    assert fc.f1a == 1
    del fc.f1a
    assert fc.f1 == None
    assert fc.f1a == None


def test_default_value():
    class FC(ar.FieldContainer):
        f = ar.Field(default = 10)
    fc = FC()

    assert fc.f == 10
    fc.f = 1
    assert fc.f == 1
    del fc.f
    assert fc.f == 10
    del fc.f
    assert fc.f == 10


def test_field_to_doc():
    class FC(ar.FieldContainer):
        f1 = ar.Field()
    fc = FC()

    e = fc.to_doc()
    assert e["f1"] == None

    fc.f1 = 1
    e = fc.to_doc()
    assert e["f1"] == 1


def test_field_from_doc():
    class FC(ar.FieldContainer):
        f1 = ar.Field()

    doc = { "f1": "hello" }
    fc = FC.from_doc(doc)

    assert fc.f1 == "hello"
