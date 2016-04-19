import pytest

import aiorethink
from aiorethink import ValidationError, IllegalValidatorError
from aiorethink.validatable import Validatable



class IntValidatable(Validatable):
    def _validate(self, val):
        if type(val) != int:
            raise ValidationError("not an integer")
        return val

class EvenValidatable(IntValidatable):
    def _validate(self, val):
        if val % 2 != 0:
            raise ValidationError("not even")
        return val



def test_simple():
    a = IntValidatable()
    assert a.validate(4) == 4
    with pytest.raises(ValidationError):
        a.validate("hello")


def test_subclass():
    a = EvenValidatable()
    assert a.validate(4) == 4
    with pytest.raises(ValidationError):
        a.validate(3)
    with pytest.raises(ValidationError):
        a.validate("hello")
    with pytest.raises(ValidationError):
        a.validate(1.5)


def test_uses_self_validator():
    class ValidatesSelfInt(Validatable):
        def setval(self, val):
            self.val = val

        def _validate(self):
            if type(self.val) != int:
                raise ValidationError("not int")
            return self

    a = ValidatesSelfInt()
    a.setval(4)
    assert a.validate() == a
    assert a.validate(0) == a
    a.setval("hello")
    with pytest.raises(ValidationError):
        a.validate()


def test_invalid_validator():
    class InvalidValidator(Validatable):
        def _validate(self, arg1, arg2):
            return arg1

    a = InvalidValidator()
    with pytest.raises(IllegalValidatorError):
        a.validate(0)


def test_extra_validators():
    def odd_validator(_, val):
        if val % 2 != 1:
            raise ValidationError("not odd")
        return val

    a = IntValidatable(extra_validators = (odd_validator,))
    assert a.validate(3) == 3
    with pytest.raises(ValidationError):
        a.validate(4)
    with pytest.raises(ValidationError):
        a.validate("hello")


def test_too_many_kwargs():
    with pytest.raises(ValueError):
        a = EvenValidatable(arg1 = "hello")
