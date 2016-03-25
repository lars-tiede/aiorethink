import inspect

from .errors import ValidationError, IllegalValidatorError

__all__ = [ "Validatable" ]


class Validatable:
    """Base class encapsulating validation for Fields, Documents etc.
    
    Subclasses that want to specify their own validation can override
    _validate(). Instances can further customize validation by tacking on an
    iterable of extra_validator functions on object creation.
    
    extra_validators, if present, must be an iterable of callable which each
    accept one or two parameters: the object the validation runs on (think
    "self"), and optionally the value to be validated. Field classes have to
    use the second form, whereas Document classes can use the first because
    there, the object to be validated and the value to be validated are the
    same. Let the validators return the validated value.

    When validation goes wrong, validation functions are expected to raise a
    ValidationError.
    
    Validation can be run using validate(), which runs all _validate()
    functions that are defined in the class hierarchy, plus the optional
    _extra_validators.
    """

    def __init__(self, **kwargs):
        """Subclasses must pop all their args off kwargs before calling this.

        Optional kwargs:
        extra_validators: iterable of extra validators tacked onto the object.
            See Validatable class doc for more on this.
        """
        self._extra_validators = kwargs.pop("extra_validators", None)

        # make sure that we didn't get any kwargs we don't handle
        if len(kwargs) > 0:
            raise ValueError("unknown kwargs {}".format(kwargs.keys()))


    def _validate(self, val):
        """Override this in subclasses where you want validation. No need to
        call super()._validate.

        Overridden _validate function may take one or two parameters: "self"
        (i.e. the object on which validation runs), and the value that is to be
        validated. In Field classes, you need that second argument. In Document
        classes, 'self' is usually the value to be validated, so there you can
        just omit the value.

        Return the validated value (i.e. self or val, or slightly transformed
        val if your validation does subtle things to val).
        """
        return val


    def validate(self, val = None):
        """Runs all validators, beginning with the most basic _validate (the
        one defined highest up in the class hierarchy), and ending with the
        extra_validators that might be attached to the object. The validated
        value is returned.

        Aiorethink users shouldn't *have to* call this function directly, as
        aiorethink should call it implicitly when necessary.
        """
        validators = self._find_methods_in_reverse_mro("_validate")
        if self._extra_validators != None:
            validators.extend(self._extra_validators)

        for validator in validators:
            params = inspect.signature(validator).parameters
            if len(params) == 1:
                val = validator(self)
            elif len(params) == 2:
                val = validator(self, val)
            else:
                raise IllegalValidatorError("validator function {} has too "
                    "many arguments".format(validator))
        return val


    @classmethod
    def _find_methods_in_reverse_mro(cls, name):
        """Collects methods with matching name along the method resolution
        order in a list, reverses that list, and returns it.
        """
        # we have a cache for this. See if we get a hit for name
        cache = cls.__dict__.get("_find_methods_cache", None)
        if cache != None:
            methods = cache.get(name, None)
            if methods != None:
                return methods
        else:
            cls._find_methods_cache = cache = {}

        # still here? then we need to do the work
        methods = []
        for c in cls.__mro__:
            method = c.__dict__.get(name, None)
            if method != None:
                methods.append(method)
        methods.reverse()
        cache[name] = methods
        return methods
