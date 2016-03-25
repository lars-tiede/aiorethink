import re

from ..errors import ValidationError
from .base import Field

__all__ = [ "StringField" ]


class TypedField(Field):
    """You can use this as a base for any Field whose values must be an
    instance of some type. Just override the classvar _val_type in your
    subclass.
    """
    _val_type = type(None)

    def _validate(self, val):
        if val == None:
            return val

        oktype = self.__class__._val_type
        if not isinstance(val, oktype):
            raise ValidationError("value is not a {}".format(oktype))

        return val


class IntField(TypedField):
    _val_type = int


class StringField(TypedField):
    _val_type = str

    def __init__(self, **kwargs):
        self._max_length = kwargs.pop("max_length", None)
        self._regex = kwargs.pop("regex", None)
        if self._regex:
            self._regex = re.compile(self._regex)
        super().__init__(**kwargs)

    def _validate(self, val):
        if val == None:
            return val

        if self._max_length and len(val) > self._max_length:
            raise ValidationError("string is too long ({} chars)"
                    .format(len(val)))

        if self._regex and not self._regex.search(val):
            raise ValidationError("string does not match validation regex")

        return val
