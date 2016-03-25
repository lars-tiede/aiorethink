import pytest

import aiorethink
from aiorethink import db_conn


def test_basic_properties(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field()

    assert Doc.f1.default == None
    assert Doc.f1.name == "f1"
    assert Doc.f1.dbname == "f1"
    assert Doc.f1.required == False
    assert Doc.f1.indexed == False
    assert Doc.f1.primary_key == False


def test_repr(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field()

    assert "name=f1" in repr(Doc.f1)


def test_simple_get_set_delete(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field()
    d = Doc()

    assert d.f1 == None
    d.f1 = 1
    assert d.f1 == 1
    del d.f1
    assert d.f1 == None
    del d.f1
    assert d.f1 == None


def test_default_value(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field(default = 10)
    d = Doc()

    assert d.f1 == 10
    d.f1 = 1
    assert d.f1 == 1
    del d.f1
    assert d.f1 == 10
    del d.f1
    assert d.f1 == 10


def test_custom_name(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field(name = "field1")

    assert Doc.f1.name == "f1"
    assert Doc.f1.dbname == "field1"


def test_fail_if_indexed_and_primary_key(aiorethink_session):
    with pytest.raises(aiorethink.IllegalSpecError):
        class Doc(aiorethink.Document):
            f1 = aiorethink.Field(indexed = True, primary_key = True)


def test_required(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field(required = True)
    d = Doc()

    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f1")
    d.f1 = 1
    assert d.validate_field("f1") == 1
    del d.f1
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f1")


def test_to_doc(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field()
    d = Doc()

    e = d.to_doc()
    assert e["f1"] == None

    d.f1 = 1
    e = d.to_doc()
    assert e["f1"] == 1


def test_from_doc(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.Field()

    doc = { "f1": "hello" }
    d = Doc.from_doc(doc, False)

    assert d.f1 == "hello"
