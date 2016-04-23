import pytest

import aiorethink as ar


###############################################################################
# AnyValueType
###############################################################################

def test_trivial_value_type():
    vt = ar.AnyValueType()

    testvals = [None, 0, 1, False, "hello", [1,2,3], {1,2,3},
            ar.AnyValueType()]

    for testval in testvals:
        assert vt.validate(testval) == vt
        assert vt.pyval_to_dbval(testval) == testval
        assert vt.dbval_to_pyval(testval) == testval


def test_any_with_custom_validator():
    def validate_int(vt, val):
        if not isinstance(val, int):
            raise ar.ValidationError

    def validate_even(vt, val):
        if val % 2 != 0:
            raise ar.ValidationError

    vt = ar.AnyValueType(extra_validators = [validate_int, validate_even])

    assert vt.validate(2) == vt
    with pytest.raises(ar.ValidationError):
        vt.validate(3)
    with pytest.raises(ar.ValidationError):
        vt.validate(True)


def test_any_with_stop_validation():
    def validate_odd(vt, val):
        if val % 2 != 1:
            raise ar.ValidationError
        raise ar.StopValidation

    def validate_3(vt, val):
        if val % 3 != 0:
            raise ar.ValidationError

    vt = ar.AnyValueType(extra_validators = [validate_odd, validate_3])

    assert vt.validate(5) == vt


def test_any_forbid_none():
    vt1 = ar.AnyValueType(forbid_none = False)
    vt2 = ar.AnyValueType(forbid_none = True)

    assert vt1.validate(None) == vt1
    with pytest.raises(ar.ValidationError):
        vt2.validate(None)


###############################################################################
# simple TypedValueTypes
###############################################################################

def test_int():
    vt = ar.IntValueType()
    assert vt.validate(0) == vt
    assert vt.validate(None) == vt
    with pytest.raises(ar.ValidationError):
        vt.validate("abc")


def test_string_simple():
    vt = ar.StringValueType()
    assert vt.validate("Hello") == vt
    assert vt.validate(None) == vt
    with pytest.raises(ar.ValidationError):
        vt.validate(0)


def test_string_maxlen():
    vt = ar.StringValueType(max_length = 3)
    assert vt.validate("Hel") == vt
    assert vt.validate(None) == vt
    with pytest.raises(ar.ValidationError):
        vt.validate("Hello")


def test_string_re():
    vt = ar.StringValueType(regex = "^[a-z]+$")
    assert vt.validate("abcde")
    with pytest.raises(ar.ValidationError):
        vt.validate("ABCDE")
    with pytest.raises(ar.ValidationError):
        vt.validate(None)


def test_string_re_and_maxlen():
    vt = ar.StringValueType(max_length = 3, regex = "^[a-z]+$")
    assert vt.validate("abc")
    with pytest.raises(ar.ValidationError):
        vt.validate("abcde")
    with pytest.raises(ar.ValidationError):
        vt.validate("ABC")
    with pytest.raises(ar.ValidationError):
        vt.validate(None)
