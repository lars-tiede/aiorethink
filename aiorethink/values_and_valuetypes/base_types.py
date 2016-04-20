from ..errors import StopValidation, ValidationError


__all__ = ["AnyValueType", "TypedValueType"]


class AnyValueType:
    """An instance of ValueType describes for some type of value (int, "int
    between 0 and 10", list, "list with elements of value type
    int-between-0-and-10", dict, namedtuple, whathaveyou):
    * how to make sure some value is of that type (*validation*)
    * how values of that type are represented in the DB and Python worlds, and
      how to convert safely between those representations (*conversion*).
    
    `AnyValueType` itself is the most general value type that validates any
    Python object and does not convert anything, unless it is specialized in
    some way.

    A ValueType object does not hold any data. Therefore, the validator and
    converter methods all accept an extra parameter for the actual value to be
    validated/converted.

    Validation
    ----------

    We say that some value "is of the value type X" if it passes X's
    ``validate()`` method without raising a ``ValidationError``.

    Validation can be run simply by calling ``some_value_type.validate(val)``,
    which runs all ``_validate()`` methods that are defined in
    `some_value_type`'s class and ancestor classes, plus the optional
    ``_extra_validators`` that instances of ValueType can tack on.

    Through subclassing of ValueTypes and/or tacking on _extra_validators on
    value type objects, value types can get arbitrarily special.

    Conversion
    ----------

    Any value has two representations (which might be the same): a "DB world"
    representation and a "Python world" representation. The DB world
    representation is something JSON serializable that RethinkDB can store and
    load. The Python world representation is some Python object that can be
    constructed from / written to a DB world representation.

    A ValueType implements ``pyval_to_dbval`` and ``dbval_to_pyval`` methods
    that transform values between these two worlds.

    In cases where "Python world" and "DB world" representations are the same,
    for instance integers or strings, these methods just return the value
    passed to them unchanged.

    Subclassing: validation
    -----------------------

    Subclasses that want to specify their own validation should override
    ``_validate``. 
    
    extra_validators, if present, must be an iterable of callable which each
    accept two parameters: the ValueType object the validation runs on (think
    "self"), and the value to be validated.

    See ``_validate`` for details on validator functions.

    Subclassing: conversion
    -----------------------

    If your ValueType needs conversion (i.e. if Python world and DB world
    representation differ), then override ``pyval_to_dbval`` and
    ``dbval_to_pyval``.
    """

    def __init__(self, extra_validators = None, forbid_none = False):
        """Optional kwargs:
        * extra_validators: iterable of extra validators tacked onto the
            object. See ``ValueType`` class doc for more on this.
        * forbid_none: set to True if the value None should always fail
            validation. The default is False.
        """
        self._extra_validators = extra_validators
        self._forbid_none = forbid_none


    @classmethod
    def _find_methods_in_reverse_mro(cls, name):
        """Collects methods with matching name along the method resolution
        order in a list, reverses that list, and returns it.
        """
        # we have a cache for this. See if we get a hit for name
        cache = cls.__dict__.get("_find_methods_cache", None)
        if cache == None:
            cls._find_methods_cache = cache = {}
        else:
            methods = cache.get(name, None)
            if methods != None:
                return methods

        # still here? then we need to do the work
        methods = []
        for c in cls.__mro__:
            method = c.__dict__.get(name, None)
            if method != None:
                methods.append(method)
        methods.reverse()
        cache[name] = methods
        return methods


    ###########################################################################
    # validation
    ###########################################################################

    def _validate(self, val):
        """Override this in subclasses where you want validation. Don't call
        super()._validate.

        If validation goes wrong, raise a ValidationError.

        If you need the validation cascade to stop after this validator, raise
        StopValidation.
        """
        if val == None and self._forbid_none:
            raise ValidationError("None is not an allowed value.")


    def validate(self, val = None):
        """Runs all validators, beginning with the most basic _validate (the
        one defined furthest back in the method resolution order), and ending
        with the extra_validators that might be attached to the object. The
        method returns self.

        Aiorethink users don't have to call this function directly, as
        aiorethink calls it implicitly when necessary.
        """
        validators = self.__class__._find_methods_in_reverse_mro("_validate")

        if self._extra_validators != None:
            validators = validators[:]
            validators.extend(self._extra_validators)

        for validator in validators:
            try:
                validator(self, val)
            except StopValidation as s:
                break
        return self


    ###########################################################################
    # conversions RethinkDB doc <-> Python object
    ###########################################################################

    def pyval_to_dbval(self, pyval):
        """Converts a "python world" pyval to a "DB world" value (i.e.,
        something JSON serializable that RethinkDB can store). Override in
        subclasses. The default implementation just returns pyval.
        """
        return pyval

    def dbval_to_pyval(self, dbval):
        """Converts a "DB world" dbval to a "Python world" value (i.e., some
        Python object constructed from it). Override in subclasses. The default
        implementation just returns dbval.
        """
        return dbval



class TypedValueType(AnyValueType):
    """Base for ValueTypes whose validation simply checks if a value
    isinstance() of a given type. Just override _val_instance_of with a type.
    """
    _val_instance_of = type(None)

    def _validate(self, val):
        oktype = self._val_instance_of
        if val != None and not isinstance(val, oktype):
            raise ValidationError("value {} is not an instance of {}, but "
                "{}".format(repr(val), str(oktype), str(val.__class__)))
