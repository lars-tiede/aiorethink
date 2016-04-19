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
    def validate_even(vt, val):
        if val % 2 != 0:
            raise ar.ValidationError
        raise ar.StopValidation

    def validate_4(vt, val):
        if val % 4 != 0:
            raise ar.ValidationError

    vt = ar.AnyValueType(extra_validators = [validate_even, validate_4])

    assert vt.validate(2) == vt


###############################################################################
# simple TypedValueTypes
###############################################################################
# TODO
