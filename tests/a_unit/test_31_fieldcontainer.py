import asyncio

import pytest
import rethinkdb as r

import aiorethink as ar


@pytest.fixture
def EmptyFC():
    class EmptyFC(ar.FieldContainer):
        pass
    return EmptyFC


def test_empty_fc_class(EmptyFC):
    fc = EmptyFC()
    assert len(fc) == 0
    assert fc.len(ar.ALL) == 0
    assert fc.len(ar.UNDECLARED_ONLY) == 0
    assert fc.len(ar.DECLARED_ONLY) == 0


def test_repr(EmptyFC):
    fc = EmptyFC()
    r = repr(fc)
    r = str(fc)


###############################################################################
# fields
###############################################################################

def test_just_undeclared_fields(EmptyFC):
    fc1 = EmptyFC(f1 = "f1val", f2 = 2)
    fc2 = EmptyFC(f2 = 2)
    fc2["f1"] = "f1val"
    fc3 = EmptyFC()
    fc3["f1"] = "f1val"
    fc3["f2"] = 2

    for fc in [fc1, fc2, fc3]:
        assert len(fc) == 2
        assert fc.len(ar.ALL) == 2
        assert fc.len(ar.UNDECLARED_ONLY) == 2
        assert fc.len(ar.DECLARED_ONLY) == 0
        assert fc["f1"] == "f1val"
        assert fc["f2"] == 2


@pytest.fixture
def FCWithFields(aiorethink_session):
    class FCWithFields(ar.FieldContainer):
        f1 = ar.Field()
        f2 = ar.Field()
    return FCWithFields


def test_just_declared_fields(FCWithFields):
    fc = FCWithFields()
    assert len(fc) == 2
    assert fc.f1 == None
    fc.f1 = 1
    assert fc.f1 == 1
    fc["f1"] = 2
    assert fc.f1 == 2
    assert len(fc) == 2


def test_mixed_fields(FCWithFields):
    fc = FCWithFields()
    fc["f3"] = 3
    assert len(fc) == 3


def test_invalid_field_name(aiorethink_session):
    with pytest.raises(ar.IllegalSpecError):
        class MyFC(ar.FieldContainer):
            items = ar.Field()


###############################################################################
# dict-like interface
###############################################################################

@pytest.fixture
def fc_mixed(FCWithFields):
    return FCWithFields(f1 = 1, f2 = 2, f3 = 3, f4 = 4)


def test_contains(fc_mixed):
    d = fc_mixed
    assert "f1" in d
    assert "f4" in d
    assert "f0" not in d


def test_get(fc_mixed):
    d = fc_mixed
    assert d.get("f1") == 1
    assert d.get("f1", 2) == 1
    assert d.get("f4", 2) == 4
    assert d.get("blah") == None
    assert d.get("blah", 2) == 2


def test_delitem(fc_mixed):
    d = fc_mixed

    # delete a declared field: will effectively set it to its default
    assert d.f2 == 2
    assert len(d) == 4
    del d.f2
    assert d.f2 == None
    assert len(d) == 4
    assert "f2" in d
    d["f2"] = 2
    assert len(d) == 4
    assert d.f2 == 2
    del d["f2"]
    assert d.f2 == None
    assert len(d) == 4
    assert "f2" in d

    # delete an undeclared field: field disappears
    assert d["f3"] == 3
    del d["f3"]
    with pytest.raises(KeyError):
        v = d["f3"]
    assert len(d) == 3
    assert "f3" not in d


def test_iter(fc_mixed):
    keys = [ k for k in fc_mixed ]
    keys.sort()
    assert keys == ["f1", "f2", "f3", "f4"]


def test_keys(fc_mixed):
    keys = list(fc_mixed.keys(ar.ALL))
    keys.sort()
    assert keys == ["f1", "f2", "f3", "f4"]

    keys = list(fc_mixed.keys(ar.DECLARED_ONLY))
    keys.sort()
    assert keys == ["f1", "f2"]

    keys = list(fc_mixed.keys(ar.UNDECLARED_ONLY))
    keys.sort()
    assert keys == ["f3", "f4"]


def test_values(fc_mixed):
    values = list(fc_mixed.values(ar.ALL))
    values.sort()
    assert values == [1, 2, 3, 4]

    values = list(fc_mixed.values(ar.DECLARED_ONLY))
    values.sort()
    assert values == [1, 2]

    values = list(fc_mixed.values(ar.UNDECLARED_ONLY))
    values.sort()
    assert values == [3, 4]


def test_items(fc_mixed):
    items = fc_mixed.items(ar.ALL)
    assert len(items) == 4
    for k, v in items:
        assert fc_mixed[k] == v


def test_clear(fc_mixed):
    for i in range(2):
        fc_mixed.clear()
        assert len(fc_mixed) == 2
        assert fc_mixed.len(ar.UNDECLARED_ONLY) == 0
        assert fc_mixed.f1 == None
        with pytest.raises(KeyError):
            v = fc_mixed["f3"]


def test_copy(fc_mixed):
    d = fc_mixed
    c = d.copy()
    assert c.f1 == d["f1"]
    assert c["f2"] == d.f2
    assert c["f3"] == d["f3"]
    assert c["f4"] == d["f4"]
    assert len(c) == len(d)
    assert c.keys() == d.keys()
    assert c.values() == d.values()

    c = d.copy(ar.DECLARED_ONLY)
    assert c.f1 == d["f1"]
    assert c["f2"] == d.f2
    with pytest.raises(KeyError):
        v = c["f3"]
    assert len(c) < len(d)

    c = d.copy(ar.UNDECLARED_ONLY)
    assert c.f1 == c.f2 == None
    assert c.f1 != d["f1"]
    assert c["f3"] == d["f3"]
    assert len(c) == len(d)


def test_update(fc_mixed):
    d = {"f1" : True,
            "f3": True,
            "f5": True}
    fc_mixed.update(d, f6 = True)
    assert len(fc_mixed) == 6
    assert fc_mixed.f1 == True
    assert fc_mixed.f2 == 2
    assert fc_mixed["f3"] == True
    assert fc_mixed["f5"] == True
    assert fc_mixed["f6"] == True


def test_update2(fc_mixed):
    d = {"f1" : True,
            "f3": True,
            "f5": True}
    fc_mixed.update(d.items())
    assert len(fc_mixed) == 5
    assert fc_mixed.f1 == True
    assert fc_mixed.f2 == 2
    assert fc_mixed["f3"] == True
    assert fc_mixed["f5"] == True


###############################################################################
# more dict-like interface: access to "DB world" keys / values
###############################################################################

@pytest.fixture
def FCWithSpecialDBFieldNames(aiorethink_session):
    class FCWithSpecialDBFieldNames(ar.FieldContainer):
        f1 = ar.Field(name = "field1")
    return FCWithSpecialDBFieldNames


@pytest.fixture
def fc_special_dbname(FCWithSpecialDBFieldNames):
    return FCWithSpecialDBFieldNames(f1 = 1, f2 = 2)


def test_get_key_for_dbkey(fc_special_dbname):
    d = fc_special_dbname
    assert d.get_key_for_dbkey("field1") == "f1"
    assert d.get_key_for_dbkey("f2") == "f2"


def test_dbkeys(fc_special_dbname):
    d = fc_special_dbname
    dbkeys = d.dbkeys()
    keys = d.keys()
    assert dbkeys != keys
    assert "field1" in dbkeys
    assert "field1" not in keys
    assert "f1" not in dbkeys
    assert "f1" in keys


def test_setitem_fails_when_trying_to_make_undeclared_field_with_existing_dbname(fc_special_dbname):
    with pytest.raises(ar.AlreadyExistsError):
        fc_special_dbname["field1"] = True


@pytest.fixture
def FCWithSpecialDBReprField(FCWithSpecialDBFieldNames):
    class SwapCaseValueType(ar.StringValueType):
        def pyval_to_dbval(self, val):
            return val.swapcase()
        dbval_to_pyval = pyval_to_dbval 

    class FCWithSpecialDBReprField(FCWithSpecialDBFieldNames):
        f_swapcase = ar.Field(SwapCaseValueType(), default = "")

    return FCWithSpecialDBReprField

@pytest.fixture
def fc_with_swapcase_field(FCWithSpecialDBReprField):
    return FCWithSpecialDBReprField(f2 = "Bla", f_swapcase = "Hello")


def test_get_dbvalue(fc_with_swapcase_field):
    d = fc_with_swapcase_field
    assert d.get("f_swapcase") == "Hello"
    assert d.get_dbvalue("f_swapcase") == "hELLO"
    assert d.get_dbvalue("f2") == "Bla"
    assert d.get_dbvalue("not_exist") == None
    assert d.get_dbvalue("not_exist", 1) == 1


def test_set_dbvalue(fc_with_swapcase_field):
    d = fc_with_swapcase_field
    d.set_dbvalue("f_swapcase", "World")
    d.set_dbvalue("f2", "World")
    
    assert d.get("f_swapcase") == "wORLD"
    assert d.get("f2") == "World"
    assert d.get_dbvalue("f_swapcase") == "World"
    assert d.get_dbvalue("f2") == "World"


def test_dbvalues(fc_with_swapcase_field):
    d = fc_with_swapcase_field
    d.f1 = "z"
    values = list(d.dbvalues(ar.ALL))
    values.sort()
    assert values == ["Bla", "hELLO", "z"]

    values = list(d.dbvalues(ar.DECLARED_ONLY))
    values.sort()
    assert values == ["hELLO", "z"]

    values = list(d.dbvalues(ar.UNDECLARED_ONLY))
    values.sort()
    assert values == ["Bla"]


def test_dbitems(fc_with_swapcase_field):
    d = fc_with_swapcase_field
    items = d.dbitems()
    assert len(items) == 3
    for k, v in items:
        assert d.get_dbvalue(k) == v


def test_to_doc(fc_with_swapcase_field):
    d = fc_with_swapcase_field
    e = d.to_doc()
    assert set(e.keys()) == set(d.dbkeys())
    for k, v in e.items():
        assert d.get_dbvalue(d.get_key_for_dbkey(k)) == v


###############################################################################
# validation
###############################################################################

def test_validate_a_field(fc_mixed):
    assert fc_mixed == fc_mixed.validate_field("f1")
    assert fc_mixed == fc_mixed.validate_field("f2")
    with pytest.raises(ValueError):
        fc_mixed.validate_field("f3")
    with pytest.raises(ValueError):
        fc_mixed.validate_field("field_that_does_not_exist")


def test_validate_simple_fc(fc_mixed):
    fc_mixed.validate()
    fc_mixed.f2 = "hello"
    fc_mixed["f4"] = 0
    fc_mixed["new_field"] = "blah"
    fc_mixed.validate()
    fc_mixed.validate()


@pytest.fixture
def FCEvenValidator(FCWithFields):
    class FCIntValidator(FCWithFields):
        def validate(self):
            super().validate()
            if type(self.f1) != int or type(self.f2) != int:
                raise ar.ValidationError("f1 or f2 is not int")
    class FCEvenValidator(FCIntValidator):
        def validate(self):
            super().validate()
            if (self.f1 + self.f2) % 2 != 0:
                raise ar.ValidationError("f1 + f2 not even")
    return FCEvenValidator


def test_validate_fc(FCEvenValidator):
    d = FCEvenValidator(f1 = 1, f2 = 3, f3 = 1)
    d.validate()
    d.f2 = 5
    d.validate()


def test_fail_validation_not_even(FCEvenValidator):
    d = FCEvenValidator(f1 = 1, f2 = 3)
    d.validate()
    d.f1 += 1
    with pytest.raises(ar.ValidationError):
        d.validate()


def test_fail_validation_not_int(FCEvenValidator):
    d = FCEvenValidator(f1 = 1, f2 = "hello")
    with pytest.raises(ar.ValidationError):
        d.validate()


###############################################################################
# loading field containers from documents and DB
###############################################################################

def test_from_doc(EmptyFC,
        FCWithFields,
        fc_mixed,
        fc_special_dbname,
        fc_with_swapcase_field):
    fc_empty = EmptyFC()
    fc_with_fields = FCWithFields()

    for fc in [fc_empty, fc_with_fields, fc_mixed, fc_special_dbname,
            fc_with_swapcase_field]:
        db_doc = fc.to_doc()
        fc2 = fc.__class__.from_doc(db_doc)
        for k, v in fc.items():
            assert fc2[k] == v
        for k, v in fc2.items():
            assert fc[k] == v


@pytest.mark.asyncio
async def test_from_query(FCWithFields, db_conn, aiorethink_db_session):
    cn = await db_conn
    await r.table_create("test", durability="soft").run(cn)

    # zero or one result: no result
    res = await FCWithFields.from_query(r.table("test").get(1))
    assert res == None

    # zero or one result: one result
    d = FCWithFields(f1 = 1, f2 = 0, id = 1)
    await r.table("test").insert(d.to_doc()).run(cn)
    res = await FCWithFields.from_query(r.table("test").get(1))
    assert isinstance(res, FCWithFields)
    assert res.f1 == 1
    assert res.f2 == 0
    assert res["id"] == 1

    # zero to many results
    res = await FCWithFields.from_query(r.table("test").filter({"f1": 1}))
    assert isinstance(res, ar.db.CursorAsyncMap)
    async for d in res:
        assert isinstance(d, FCWithFields)

    res = await FCWithFields.from_query(r.table("test").filter({"f1": 2}))
    assert isinstance(res, ar.db.CursorAsyncMap)
    async for d in res:
        assert isinstance(d, FCWithFields)


###############################################################################
# Value Type
###############################################################################

def test_fc_vt(FCWithFields):
    class FCWithFieldsValueType(ar.FieldContainerValueType):
        _val_instance_of = FCWithFields

    vt = FCWithFieldsValueType()

    fc = FCWithFields(f1 = 1, f2 = 2)
    assert vt.validate(fc) == vt
    assert vt.pyval_to_dbval(fc) == fc.to_doc()
    assert vt.dbval_to_pyval(fc.to_doc()) == fc
