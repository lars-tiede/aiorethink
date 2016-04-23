from .errors import IllegalAccessError, IllegalSpecError, ValidationError
from .values_and_valuetypes import AnyValueType, LazyValueType


__all__ = [ "Field", "FieldAlias" ]


class Field:
    """Field instances are attached to FieldContainer classes as class attributes.

    Field is a data descriptor, i.e. it implements __get__ and __set__ for
    attribute access on FieldContainer instances. This way, Field instances
    store values in a FieldContainer instance.

    A Field instance has an associated ValueType instance, which takes care of
    DB<->Python world value conversion and value validation.
    """

    def __init__(self, val_type = None, **kwargs):
        """``val_type`` is a ``ValueType`` or None (in which case it's just
        substituted with AnyValueType()).

        Optional kwargs:
        name: name of the property in the DB. Defaults to property name in
            python.
        indexed: if True, a secondary index will be created for this property
        required: set to True if a non-None value is required.
        primary_key: use this property as primary key. If True, indexed must be
            False and required must be True.
        default: default value (which defaults to None).
        """
        self.val_type = val_type or AnyValueType()

        self._name = None

        # get field properties from kwargs
        self._indexed = kwargs.pop("indexed", False)
        self._required = kwargs.pop("required", False)
        self._primary_key = kwargs.pop("primary_key", False)
        if self._primary_key:
            if self._indexed:
                raise IllegalSpecError("property can't be indexed *and* "
                    "primary_key")
        self._dbname = kwargs.pop("name", None)
        ## if we get a default value, make sure it's valid
        self._default = kwargs.get("default", None) # NOTE get, not pop...
        validate_default_now = False
        if "default" in kwargs:
            validate_default_now = True
            del kwargs["default"]

        # validate the default value if we have to
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
    # value access (data descriptor protocol ++)
    ###########################################################################

    def __get__(self, obj, cls):
        if obj == None:
            return self # __get__ was called on class, not instance
        return obj._declared_fields_values.get(self._name, self._default)


    def __set__(self, obj, val, mark_updated = True):
        self.validate(val)
        obj._declared_fields_values[self._name] = val
        if mark_updated:
            obj.mark_field_updated(self._name)


    def __delete__(self, obj):
        if self._name in obj._declared_fields_values:
            del obj._declared_fields_values[self._name]
            obj.mark_field_updated(self._name)


    ###########################################################################
    # conversions RethinkDB doc <-> Python object
    ###########################################################################
    # TODO rename these methods
    # TODO at least store from doc should probably be public, in order to
    # use it in changefeeds...

    def _do_convert_to_doc(self, obj):
        val = self.__get__(obj, None)
        self.validate(val) # TODO do we validate too often?
        return self.val_type.pyval_to_dbval(val)

    def _store_from_doc(self, obj, dbval, mark_updated = False):
        val = self.val_type.dbval_to_pyval(dbval)
        self.__set__(obj, val, mark_updated = mark_updated)


    ###########################################################################
    # validation
    ###########################################################################

    def validate(self, val):
        if val == None and self._required:
            raise ValidationError("no value for required property {}"
                    .format(self._name))
        else:
            self.val_type.validate(val)
            return self



class FieldAlias:
    """Use it as an alias for another Field in the same FieldContainer class,
    like so::

        class Foo(aiorethink.Document):
            f1 = aiorethink.Field(...)
            f_alias = aiorethink.FieldAlias(f1)

    aiorethink uses a FieldAlias for ``AnyDocument.pkey``, which is an alias
    for whichever field of AnyDocument is AnyDocument's primary key.

    A FieldAlias simply dispatches all attribute accesses to the target field.
    """

    def __init__(self, target):
        super().__init__()
        self._target_field = target

    def __repr__(self):
        s = "{self.__class__.__name__}(fld_name={self.name})"
        return s.format(self = self)

    __str__ = __repr__

    def __getattr__(self, name):
        return getattr(self._target_field, name)


    ###########################################################################
    # Descriptor protocol (because it's not dispatched by __getattr__)
    ###########################################################################

    def __get__(self, obj, cls):
        if obj == None:
            return self # __get__ was called on class, not instance
        return self._target_field.__get__(obj, cls)

    def __set__(self, obj, val, mark_updated = True):
        return self._target_field.__set__(obj, val, mark_updated)

    def __delete__(self, obj):
        return self._target_field.__delete__(obj)
