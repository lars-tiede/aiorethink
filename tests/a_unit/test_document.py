import pytest

import aiorethink
from aiorethink import db_conn


@pytest.fixture
def EmptyDoc(aiorethink_session):
    class EmptyDoc(aiorethink.Document):
        pass
    return EmptyDoc


def test_empty_document_class(EmptyDoc):
    doc = EmptyDoc()
    assert len(doc) == 1 # 'id' attribute we got automatically
    assert doc.len(aiorethink.ALL) == 1
    assert doc.len(aiorethink.UNDECLARED_ONLY) == 0
    assert doc.len(aiorethink.DECLARED_ONLY) == 1
    assert doc.id == None # didn't get one yet
    assert doc.stored_in_db == False
    
    assert EmptyDoc.pkey == EmptyDoc.id


def test_repr(EmptyDoc):
    doc = EmptyDoc()
    r = repr(doc)
    r = str(doc)


###############################################################################
# fields
###############################################################################

def test_just_undeclared_fields(EmptyDoc):
    doc1 = EmptyDoc(f1 = "f1val", f2 = 2)
    doc2 = EmptyDoc(f2 = 2)
    doc2["f1"] = "f1val"
    doc3 = EmptyDoc()
    doc3["f1"] = "f1val"
    doc3["f2"] = 2

    for doc in [doc1, doc2, doc3]:
        assert len(doc) == 3 # 2 fields + id
        assert doc.len(aiorethink.ALL) == 3
        assert doc.len(aiorethink.UNDECLARED_ONLY) == 2
        assert doc.len(aiorethink.DECLARED_ONLY) == 1
        assert doc["f1"] == "f1val"
        assert doc["f2"] == 2
        assert doc.stored_in_db == False


@pytest.fixture
def DocWithFields(aiorethink_session):
    class DocWithFields(aiorethink.Document):
        f1 = aiorethink.Field()
        f2 = aiorethink.Field()
    return DocWithFields


def test_just_declared_fields(DocWithFields):
    doc = DocWithFields()
    assert len(doc) == 3
    assert doc.f1 == None
    doc.f1 = 1
    assert doc.f1 == 1
    doc["f1"] = 2
    assert doc.f1 == 2
    assert len(doc) == 3


def test_mixed_fields(DocWithFields):
    doc = DocWithFields()
    doc["f3"] = 3
    assert len(doc) == 4


def test_invalid_field_name(aiorethink_session):
    with pytest.raises(aiorethink.IllegalSpecError):
        class MyDoc(aiorethink.Document):
            items = aiorethink.Field()


def test_id_but_no_primary_key(aiorethink_session):
    with pytest.raises(aiorethink.IllegalSpecError):
        class MyDoc(aiorethink.Document):
            id = aiorethink.Field()


def test_id_is_primary_key(aiorethink_session):
    class MyDoc(aiorethink.Document):
        id = aiorethink.Field(primary_key = True)

    assert MyDoc.pkey == MyDoc.id


def test_two_primary_keys(aiorethink_session):
    with pytest.raises(aiorethink.IllegalSpecError):
        class MyDoc(aiorethink.Document):
            f1 = aiorethink.Field(primary_key = True)
            f2 = aiorethink.Field(primary_key = True)


def test_custom_primary_key(aiorethink_session):
    class MyDoc(aiorethink.Document):
        f1 = aiorethink.Field(primary_key = True)
    d = MyDoc()
    assert len(d) == 1 # has only the primary key attribute
    assert not hasattr(d, "id")
    assert MyDoc.pkey == MyDoc.f1
    assert "f1" in repr(d)

    d.f1 = 1
    assert d.pkey == d.f1
    assert MyDoc.pkey == MyDoc.f1


###############################################################################
# dict-like interface
###############################################################################

@pytest.fixture
def doc_mixed(DocWithFields):
    return DocWithFields(f1 = 1, f2 = 2, f3 = 3, f4 = 4)


def test_contains(doc_mixed):
    d = doc_mixed
    assert "id" in d
    assert "f1" in d
    assert "f4" in d
    assert "f0" not in d
    assert "pkey" not in d


def test_get(doc_mixed):
    d = doc_mixed
    assert d.get("f1") == 1
    assert d.get("f1", 2) == 1
    assert d.get("f4", 2) == 4
    assert d.get("blah") == None
    assert d.get("blah", 2) == 2
    assert d.get("pkey", 1) == None


def test_delitem(doc_mixed):
    d = doc_mixed

    # delete a declared field: will effectively set it to its default
    assert d.f2 == 2
    assert len(d) == 5
    del d.f2
    assert d.f2 == None
    assert len(d) == 5
    assert "f2" in d
    d["f2"] = 2
    assert len(d) == 5
    assert d.f2 == 2
    del d["f2"]
    assert d.f2 == None
    assert len(d) == 5
    assert "f2" in d
    d.id = 1
    assert d.pkey == 1
    del d["pkey"]
    assert d.id == None
    d["pkey"] = 1
    assert d.id == 1

    # delete an undeclared field: field disappears
    assert d["f3"] == 3
    del d["f3"]
    with pytest.raises(KeyError):
        v = d["f3"]
    assert len(d) == 4
    assert "f3" not in d


def test_iter(doc_mixed):
    keys = [ k for k in doc_mixed ]
    keys.sort()
    assert keys == ["f1", "f2", "f3", "f4", "id"]


def test_keys(doc_mixed):
    keys = list(doc_mixed.keys(aiorethink.ALL))
    keys.sort()
    assert keys == ["f1", "f2", "f3", "f4", "id"]

    keys = list(doc_mixed.keys(aiorethink.DECLARED_ONLY))
    keys.sort()
    assert keys == ["f1", "f2", "id"]

    keys = list(doc_mixed.keys(aiorethink.UNDECLARED_ONLY))
    keys.sort()
    assert keys == ["f3", "f4"]


def test_values(doc_mixed):
    values = list(doc_mixed.values(aiorethink.ALL))
    values.remove(None)
    values.sort()
    assert values == [1, 2, 3, 4]

    values = list(doc_mixed.values(aiorethink.DECLARED_ONLY))
    values.remove(None)
    values.sort()
    assert values == [1, 2]

    values = list(doc_mixed.values(aiorethink.UNDECLARED_ONLY))
    values.sort()
    assert values == [3, 4]


def test_items(doc_mixed):
    items = doc_mixed.items(aiorethink.ALL)
    assert len(items) == 5
    for k, v in items:
        assert doc_mixed[k] == v


def test_clear(doc_mixed):
    for i in range(2):
        doc_mixed.clear()
        assert len(doc_mixed) == 3
        assert doc_mixed.len(aiorethink.UNDECLARED_ONLY) == 0
        assert doc_mixed.f1 == None
        with pytest.raises(KeyError):
            v = doc_mixed["f3"]


def test_copy(doc_mixed):
    d = doc_mixed
    c = d.copy()
    assert c.f1 == d["f1"]
    assert c["f2"] == d.f2
    assert c["f3"] == d["f3"]
    assert c["f4"] == d["f4"]
    assert len(c) == len(d)
    assert c.keys() == d.keys()
    assert c.values() == d.values()

    c = d.copy(aiorethink.DECLARED_ONLY)
    assert c.f1 == d["f1"]
    assert c["f2"] == d.f2
    with pytest.raises(KeyError):
        v = c["f3"]
    assert len(c) < len(d)

    c = d.copy(aiorethink.UNDECLARED_ONLY)
    assert c.f1 == c.f2 == None
    assert c.f1 != d["f1"]
    assert c["f3"] == d["f3"]
    assert len(c) == len(d)

    # don't copy primary key
    d.id = "12345"
    c = d.copy()
    assert c.id == None


def test_update(doc_mixed):
    d = {"f1" : True,
            "f3": True,
            "f5": True}
    doc_mixed.update(d, f6 = True)
    assert len(doc_mixed) == 7
    assert doc_mixed.f1 == True
    assert doc_mixed.f2 == 2
    assert doc_mixed["f3"] == True
    assert doc_mixed["f5"] == True
    assert doc_mixed["f6"] == True


def test_update2(doc_mixed):
    d = {"f1" : True,
            "f3": True,
            "f5": True}
    doc_mixed.update(d.items())
    assert len(doc_mixed) == 6
    assert doc_mixed.f1 == True
    assert doc_mixed.f2 == 2
    assert doc_mixed["f3"] == True
    assert doc_mixed["f5"] == True


###############################################################################
# more dict-like interface: access to "DB world" keys / values
###############################################################################

@pytest.fixture
def DocWithSpecialDBFieldNames(aiorethink_session):
    class DocWithSpecialDBFieldNames(aiorethink.Document):
        f1 = aiorethink.Field(name = "field1")
    return DocWithSpecialDBFieldNames


@pytest.fixture
def doc_special_dbname(DocWithSpecialDBFieldNames):
    return DocWithSpecialDBFieldNames(f1 = 1, f2 = 2)


def test_get_key_for_dbkey(doc_special_dbname):
    d = doc_special_dbname
    assert d.get_key_for_dbkey("field1") == "f1"
    assert d.get_key_for_dbkey("f2") == "f2"


def test_dbkeys(doc_special_dbname):
    d = doc_special_dbname
    dbkeys = d.dbkeys()
    keys = d.keys()
    assert dbkeys != keys
    assert "field1" in dbkeys
    assert "field1" not in keys
    assert "f1" not in dbkeys
    assert "f1" in keys


def test_setitem_fails_when_trying_to_make_undeclared_field_with_existing_dbname(doc_special_dbname):
    with pytest.raises(aiorethink.AlreadyExistsError):
        doc_special_dbname["field1"] = True


@pytest.fixture
def DocWithSpecialDBReprField(DocWithSpecialDBFieldNames):
    class SwapCaseField(aiorethink.Field):
        def _construct_from_doc(self, obj, val):
            return val.swapcase()
        def _convert_to_doc(self, obj, val):
            return val.swapcase()

    class DocWithSpecialDBReprField(DocWithSpecialDBFieldNames):
        f_swapcase = SwapCaseField(default = "")

    return DocWithSpecialDBReprField

@pytest.fixture
def doc_with_swapcase_field(DocWithSpecialDBReprField):
    return DocWithSpecialDBReprField(f2 = "Bla", f_swapcase = "Hello")


def test_get_dbvalue(doc_with_swapcase_field):
    d = doc_with_swapcase_field
    assert d.get("f_swapcase") == "Hello"
    assert d.get_dbvalue("f_swapcase") == "hELLO"
    assert d.get_dbvalue("f2") == "Bla"
    assert d.get_dbvalue("not_exist") == None
    assert d.get_dbvalue("not_exist", 1) == 1
    assert d.get_dbvalue("pkey", 1) == None


def test_set_dbvalue(doc_with_swapcase_field):
    d = doc_with_swapcase_field
    d.set_dbvalue("f_swapcase", "World")
    d.set_dbvalue("f2", "World")
    d.set_dbvalue("pkey", 1)
    
    assert d.get("f_swapcase") == "wORLD"
    assert d.get("f2") == "World"
    assert d.get("id") == d.get("pkey") == 1
    assert d.get_dbvalue("f_swapcase") == "World"
    assert d.get_dbvalue("f2") == "World"
    assert d.get_dbvalue("id") == d.get_dbvalue("pkey") == 1


def test_dbvalues(doc_with_swapcase_field):
    d = doc_with_swapcase_field
    d.f1 = "z"
    values = list(d.dbvalues(aiorethink.ALL))
    values.remove(None)
    values.sort()
    assert values == ["Bla", "hELLO", "z"]

    values = list(d.dbvalues(aiorethink.DECLARED_ONLY))
    values.remove(None)
    values.sort()
    assert values == ["hELLO", "z"]

    values = list(d.dbvalues(aiorethink.UNDECLARED_ONLY))
    values.sort()
    assert values == ["Bla"]


def test_dbitems(doc_with_swapcase_field):
    d = doc_with_swapcase_field
    items = d.dbitems()
    assert len(items) == 4
    for k, v in items:
        assert d.get_dbvalue(k) == v


def test_to_doc(doc_with_swapcase_field):
    d = doc_with_swapcase_field
    e = d.to_doc()
    assert set(e.keys()) == set(d.dbkeys())
    for k, v in e.items():
        assert d.get_dbvalue(d.get_key_for_dbkey(k)) == v


###############################################################################
# validation
###############################################################################

def test_validate_a_field(doc_mixed):
    assert doc_mixed.f1 == doc_mixed.validate_field("f1")
    assert doc_mixed.f2 == doc_mixed.validate_field("f2")
    assert doc_mixed.id == doc_mixed.validate_field("id")
    with pytest.raises(ValueError):
        doc_mixed.validate_field("f3")
    with pytest.raises(ValueError):
        doc_mixed.validate_field("field_that_does_not_exist")


def test_validate_simple_document(doc_mixed):
    doc_mixed.validate()
    doc_mixed.f2 = "hello"
    doc_mixed["f4"] = 0
    doc_mixed["new_field"] = "blah"
    doc_mixed.validate()
    doc_mixed.validate()


@pytest.fixture
def DocEvenValidator(DocWithFields):
    class DocIntValidator(DocWithFields):
        def _validate(self):
            if type(self.f1) != int or type(self.f2) != int:
                raise aiorethink.ValidationError("f1 or f2 is not int")
    class DocEvenValidator(DocIntValidator):
        def _validate(self):
            if (self.f1 + self.f2) % 2 != 0:
                raise aiorethink.ValidationError("f1 + f2 not even")
    return DocEvenValidator


def test_validate_document(DocEvenValidator):
    d = DocEvenValidator(f1 = 1, f2 = 3, f3 = 1)
    d.validate()
    d.f2 = 5
    d.validate()


def test_fail_validation_not_even(DocEvenValidator):
    d = DocEvenValidator(f1 = 1, f2 = 3)
    d.validate()
    d.f1 += 1
    with pytest.raises(aiorethink.ValidationError):
        d.validate()


def test_fail_validation_not_int(DocEvenValidator):
    d = DocEvenValidator(f1 = 1, f2 = "hello")
    with pytest.raises(aiorethink.ValidationError):
        d.validate()
