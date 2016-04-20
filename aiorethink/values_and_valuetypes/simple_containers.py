import collections

from ..errors import IllegalSpecError
from .base_types import AnyValueType, TypedValueType

__all__ = ["TupleValueType", "ListValueType", "SetValueType", "DictValueType",
        "NamedTupleValueType" ]


###############################################################################
# The basic ones: tuple, list, set, dict
###############################################################################

class ElementContainerValueType(TypedValueType):
    """Base for validation and conversion for element containers (tuple, set,
    list) whose elements have some value type.

    If the value type for the elements is not specified, then ``AnyValueType``
    is used, which in practical terms means that elements can be of any type,
    any value is considered valid, and there is no conversion between DB and
    Python world values.

    In the DB, element containers are represented as lists.
    """

    def __init__(self, elem_type = None, **kwargs):
        super().__init__(**kwargs)
        self._elem_type = elem_type or AnyValueType()
        if not isinstance(self._elem_type, AnyValueType):
            raise TypeError("elem_type must be a ValueType")


    def _validate(self, val):
        for e in val:
            self._elem_type.validate(e)


    def dbval_to_pyval(self, dbval):
        if dbval == None:
            return None
        return self._val_instance_of(
                self._elem_type.dbval_to_pyval(v) for v in dbval)


    def pyval_to_dbval(self, pyval):
        if pyval == None:
            return None
        return [ self._elem_type.pyval_to_dbval(v) for v in pyval ]


class TupleValueType(ElementContainerValueType):
    """Validation and conversion for tuples whose elements have some value type.
    """
    _val_instance_of = tuple


class ListValueType(ElementContainerValueType):
    """Validation and conversion for lists whose elements have some value type.
    """
    _val_instance_of = list


class SetValueType(ElementContainerValueType):
    """Validation and conversion for sets whose elements have some value type.
    """
    _val_instance_of = set



class DictValueType(TypedValueType):
    """Validation and conversion for dicts whose keys and elements have some
    value type.
    """
    _val_instance_of = dict

    def __init__(self, key_type = None, val_type = None, **kwargs):
        super().__init__(**kwargs)
        self._key_type = key_type or AnyValueType()
        self._val_type = val_type or AnyValueType()


    def _validate(self, val):
        for k, v in val.items():
            self._key_type.validate(k)
            self._val_type.validate(v)


    def dbval_to_pyval(self, dbval):
        if dbval == None:
            return None
        return { self._key_type.dbval_to_pyval(k): self._val_type.dbval_to_pyval(v)
                for k, v in dbval.items() }


    def pyval_to_dbval(self, pyval):
        if pyval == None:
            return None
        return { self._key_type.pyval_to_dbval(k): self._val_type.pyval_to_dbval(v)
                for k, v in pyval.items() }


###############################################################################
# Slightly more "advanced" ones: namedtuple, ...? (maybe OrderedDict one day?)
###############################################################################

class NamedTupleValueType(TypedValueType):
    """Validation and conversion for `collections.namedtuple`s. Each tuple item
    has its own value type.

    The DB representation of a namedtuple is a dictionary.
    """

    def __init__(self, tuple_type, item_value_types = None, **kwargs):
        """`tuple_type` is the type you get when you call
        `collections.namedtuple()`. `item_value_types`, if specified, must be a
        list of value types, one for each item in the named tuple.
        """
        super().__init__(**kwargs)

        # tuple_type and item_value_types must have same length
        if item_value_types != None and \
                len(item_value_types) != len(tuple_type._fields):
            raise IllegalSpecError("length of item_value_types not equal "
                    "to length of tuple_type")

        self._val_instance_of = tuple_type
        self._item_value_types = item_value_types


    def _validate(self, val):
        if self._item_value_types != None:
            for item, value_type in zip(val, self._item_value_types):
                value_type.validate(item)


    def dbval_to_pyval(self, dbval):
        if dbval == None:
            return None
        return self._val_instance_of(**dbval)


    def pyval_to_dbval(self, pyval):
        if pyval == None:
            return None
        return pyval._asdict()
