import re

from ..errors import ValidationError
from .base_types import TypedValueType


__all__ = ["IntValueType", "StringValueType"]


class IntValueType(TypedValueType):
    _val_instance_of = int


class StringValueType(TypedValueType):
    _val_instance_of = str

    def __init__(self, **kwargs):
        self._max_length = kwargs.pop("max_length", None)
        self._regex = kwargs.pop("regex", None)
        if self._regex:
            self._regex = re.compile(self._regex)
        super().__init__(**kwargs)

    def _validate(self, val):
        if val != None and self._max_length and len(val) > self._max_length:
            raise ValidationError("string is too long ({} chars)"
                    .format(len(val)))

        if self._regex and not self._regex.search(val or ""):
            raise ValidationError("string does not match validation regex")
