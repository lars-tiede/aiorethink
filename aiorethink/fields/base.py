from ..validatable import Validatable
from ..errors import IllegalAccessError, IllegalSpecError, ValidationError

__all__ = [ "Field" ]


class Field(Validatable):
    def __init__(self, **kwargs):
        """optional kwargs:
        name: name of the property in the DB. Defaults to property name in
            python.
        indexed: if True, a secondary index will be created for this property
        required: set to True if a non-None value is required.
        primary_key: use this property as primary key. If True, indexed must be
            False and required must be True.
        extra_validators: iterable of callable that take two arguments (field,
            value). Must return the validated value.
        default: default value (which defaults to None).
        """
        self._name = None

        # get field properties from kwargs
        self._indexed = kwargs.pop("indexed", False)
        self._required = kwargs.pop("required", False)
        self._primary_key = kwargs.pop("primary_key", False)
        if self._primary_key:
            if self._indexed:
                raise IllegalSpecError("property can't be indexed *and* "
                    "primary_key")
            #if not self._required:
            #    raise IllegalSpecError("property can't be primary_key and not"
            #        " required")
        self._dbname = kwargs.pop("name", None)
        ## if we get a default value, make sure it's valid
        self._default = kwargs.get("default", None) # NOTE get, not pop...
        validate_default_now = False
        if "default" in kwargs:
            validate_default_now = True
            del kwargs["default"]

        # now that we popped all our args off kwargs, we call parent's
        # conctructor
        super().__init__(**kwargs)

        # finally, validate the default value if we have to
        if validate_default_now:
            try:
                self.validate(self._default)
            except ValidationError as e:
                raise IllegalSpecError from e


    ###########################################################################
    # simple properties and due diligence
    ###########################################################################

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        self._name = val

    @property
    def dbname(self):
        return self._dbname or self._name

    @property
    def indexed(self):
        return self._indexed

    @property
    def required(self):
        return self._required

    @property
    def primary_key(self):
        return self._primary_key

    @property
    def default(self):
        return self._default

    def __repr__(self):
        s = ("{cls}(name={self.name}, dbname={self.dbname}, indexed={self."
                "indexed}, required="
                "{self.required}, default={self.default}, primary_key="
                "{self.primary_key})")
        return s.format(cls = self.__class__, self = self)


    ###########################################################################
    # value access
    ###########################################################################

    def __get__(self, obj, cls):
        if obj == None:
            return self # __get__ was called on class, not instance
        return self._get_value(obj)

    def _get_value(self, obj):
        return obj._declared_fields_values.get(self._name, self._default)

    def __set__(self, obj, val, mark_updated = True):
        # NOTE referential integrity when we change primary key? hmmm...
        self._set_value(obj, val)
        if mark_updated:
            obj.mark_field_updated(self._name)

    def _set_value(self, obj, val):
        obj._declared_fields_values[self._name] = val

    def __delete__(self, obj):
        # NOTE referential integrity? hmmm...
        if self._name in obj._declared_fields_values:
            del obj._declared_fields_values[self._name]
            obj.mark_field_updated(self._name)


    ###########################################################################
    # conversions RethinkDB doc <-> Python object
    ###########################################################################

    def _do_convert_to_doc(self, obj):
        val = self._get_value(obj)
        val = self.validate(val)
        return self._convert_to_doc(obj, val)

    def _convert_to_doc(self, obj, val):
        """Converts property's value to a format suitable for storing into the
        DB (i.e. something JSON serializable). Override in subclasses. The
        default implementation just returns the property's value.
        """
        return val

    def _store_from_doc(self, obj, dbval, mark_updated = False):
        val = self._construct_from_doc(obj, dbval)
        self.__set__(obj, val, mark_updated = mark_updated)

    def _construct_from_doc(self, obj, val):
        """Construct property's value from the value stored in a document.
        Override this in subclasses. Returns the constructed object. The
        default implementation returns val.
        """
        return val


    ###########################################################################
    # validation
    ###########################################################################

    def _validate(self, val):
        if self._required and val == None:
            raise ValidationError("no value for required property {}"
                    .format(self._name))
        return val
