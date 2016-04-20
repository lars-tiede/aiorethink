import pytest

import aiorethink as ar

def test_tuple():
    vt = ar.TupleValueType(ar.IntValueType())

    t = (1,2,3)
    assert vt.validate(t) == vt
    assert vt.pyval_to_dbval(t) == [1,2,3]
    assert vt.pyval_to_dbval(None) == None
    assert vt.dbval_to_pyval([1,2,3]) == (1,2,3)
    assert vt.dbval_to_pyval(None) == None

    t = ["a", "b", "c"]
    with pytest.raises(ar.ValidationError):
        vt.validate(t)

    t = (1, 2, "a")
    with pytest.raises(ar.ValidationError):
        vt.validate(t)

    t = ("a", "b", "c")
    with pytest.raises(ar.ValidationError):
        vt.validate(t)


def test_illegal_elem_type():
    with pytest.raises(TypeError):
        vt = ar.TupleValueType(int)


def test_list():
    vt = ar.ListValueType(ar.IntValueType())

    v = [1, 2, 3]
    assert vt.validate(v) == vt
    assert vt.pyval_to_dbval(v) == [1,2,3]
    assert vt.pyval_to_dbval(None) == None
    assert vt.dbval_to_pyval([1,2,3]) == [1,2,3]
    assert vt.dbval_to_pyval(None) == None

    v = ["a", "b", "c"]
    with pytest.raises(ar.ValidationError):
        vt.validate(v)


def test_set():
    vt = ar.SetValueType(ar.IntValueType())

    v = {1, 2, 3}
    assert vt.validate(v) == vt
    assert vt.pyval_to_dbval(v) == [1,2,3]
    assert vt.pyval_to_dbval(None) == None
    assert vt.dbval_to_pyval([1,2,3]) == {1,2,3}
    assert vt.dbval_to_pyval(None) == None

    v = {"a", "b", "c"}
    with pytest.raises(ar.ValidationError):
        vt.validate(v)


def test_dict():
    vt = ar.DictValueType(ar.IntValueType(), ar.StringValueType())

    v = {1: "Hello", 2: "World"}
    assert vt.validate(v) == vt
    assert vt.pyval_to_dbval(v) == v
    assert vt.pyval_to_dbval(None) == None
    assert vt.dbval_to_pyval(v) == v
    assert vt.dbval_to_pyval(None) == None

    v = {1: "hello", "world": 2}
    with pytest.raises(ar.ValidationError):
        vt.validate(v)


def test_complex_dict():
    vt = ar.DictValueType(ar.StringValueType(regex = "^[a-z]+$"),
            ar.SetValueType(ar.StringValueType(max_length = 3)))

    v = {"a": {"abc"}, "abcd": {"xyz"}}
    assert vt.validate(v) == vt
    assert vt.pyval_to_dbval(v) == {"a": ["abc"], "abcd": ["xyz"]}
    assert vt.pyval_to_dbval(None) == None
    assert vt.dbval_to_pyval({"a": ["abc"], "abcd": ["xyz"]}) == v

    v = {"A": {"abc"}}
    with pytest.raises(ar.ValidationError):
        vt.validate(v)

    v = {"a": {"abcd"}}
    with pytest.raises(ar.ValidationError):
        vt.validate(v)

    v = {"a": ["abcd"]}
    with pytest.raises(ar.ValidationError):
        vt.validate(v)


def test_namedtuple():
    import collections

    TT = collections.namedtuple("TT", ["a", "b"])
    vt  = ar.NamedTupleValueType(TT, [ar.IntValueType(), ar.StringValueType()])
    vt2 = ar.NamedTupleValueType(TT)
    with pytest.raises(ar.IllegalSpecError):
        vt3 = ar.NamedTupleValueType(TT, [ar.IntValueType()])

    v = TT(1, "Hello")
    for _vt in [vt, vt2]:
        assert _vt.validate(v) == _vt
        assert _vt.pyval_to_dbval(v) == v._asdict()
        assert _vt.pyval_to_dbval(None) == None
        assert _vt.dbval_to_pyval(v._asdict()) == v
        assert _vt.dbval_to_pyval(None) == None

    v = TT("Hello", 1)
    assert vt2.validate(v) == vt2
    with pytest.raises(ar.ValidationError):
        vt.validate(v)
