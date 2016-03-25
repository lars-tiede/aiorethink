import functools

from ..registry import registry
from ..errors import NotLoadedError, ValidationError
from .base import Field

__all__ = [ "LazyField", "ReferenceField" ]



class LazyField(Field):
    """Field whose value can be determined on the fly on read access, possibly
    using coroutines with DB queries or other asynchronous ops.

    LazyField is an abstract class which must be subclassed. Let's call such a
    concrete subclass ConcreteLazyField.

    The value of the field is a ConcreteLazyField.Value instance, an awaitable
    object with machinery for lazy loading and caching.

    Read usage (let doc be a Document with a lazy field called my_field):
    - await doc.my_field loads and returns the field's value or returns the
      cached value if that already exists.
    - await doc.my_field.load() loads, and returns the loaded value. It also
      updates the Value instance's cached value in-place.
    - doc.my_field.get() returns the cached value, or raises NotLoadedError if
      value hasn't been loaded.
    - doc.my_field.loaded is True if value has been loaded, False otherwise
    Example:
        >>> val = await doc.my_field
        >>> # is the same as
        >>> if not doc.my_field.loaded:
        >>>     await doc.my_field.load()
        >>> val = doc.my_field.get()

    Write usage:
    - doc.my_field.set(val) or
    - doc.my_field = val
    Both do the same thing. The first version is clearer and should therefore
    be preferred. The second version has its place, though, because it provides
    the same API for setting a LazyField's value as for any other kind of
    field.


    Subclassing:
    A concrete subclass must define an inner class named Value. Check
    LazyField.Value for details on this.

    validate() functions run on the value stored inside Value, not the Value
    instance. If the value has not been loaded yet, no validation functions are
    called (in effect, a not loaded LazyField is always valid).

    Pro tip: If you want your LazyField to inherit validation from another
    non-lazy field type (say StringField), then let your subclass inherit from
    that field class, too. TODO test this
    """

    class Value:
        """Concrete subclasses of LazyField should derive and override this,
        thus define their own Value class. The methods _load() and
        _convert_to_doc() must be implemented.
        """
        def __init__(self, obj, field,
                val_doc = None,
                val_cached = None):
            self._obj = obj
            self._field = field
            self._val_doc = val_doc
            self._val_cached = val_cached
            self._loaded = (val_cached != None)

        def __repr__(self):
            s = ("{cls}(loaded={self._loaded}, val_doc={self._val_doc}, "
                    "val_cached={self._val_cached})")
            return s.format(cls = self.__class__, self = self)

        @property
        def loaded(self):
            return self._loaded

        async def load(self, conn = None):
            """Loads the value, regardless of whether it was already loaded and
            cached before. Returns the loaded value.
            """
            val_loaded = await self._load(self._val_doc, conn)
            self._val_cached = self._field._construct_from_doc(self._obj,
                    val_loaded, call_superclass = True)
            self._loaded = True
            return self._val_cached

        async def _load(self, val_doc, conn = None):
            """Override this. Return loaded value."""
            raise NotImplementedError

        def get(self):
            """Returns the cached value. Raises NotLoadedError if that doesn't
            exist.
            """
            if not self._loaded:
                raise NotLoadedError("value has not been determined yet")
            return self._val_cached

        async def _get_or_load(self):
            if self._loaded:
                return self._val_cached
            else:
                return await self.load()

        def __await__(self):
            """Returns cached value, or procures the value if it's not cached
            or caching is turned off.
            """
            return self._get_or_load().__await__()

        def set(self, new_val, mark_updated = True):
            self._val_cached = new_val
            self._loaded = True
            val_deconstructed = self._field._convert_to_doc(self._obj, new_val,
                    call_superclass = True)
            self._val_doc = self._convert_to_doc(self._val_cached)
            if mark_updated:
                self._obj.mark_field_updated(self._field._name)
            return self

        def _convert_to_doc(self, val_converted):
            """Override this. Return DB-ready value, given the 'deconstructed'
            value."""
            raise NotImplementedError


    ###########################################################################
    # value read and write access - make sure that it's always instances of
    # Value that are read or written.
    ###########################################################################

    def _get_value(self, obj):
        val = obj._declared_fields_values.get(self._name)
        if val == None:
            # make Value instance if we didn't have one before
            val = self.__class__.Value(obj, self, val_cached = self._default)
            obj._declared_fields_values[self._name] = val
        return val

    def _set_value(self, obj, val):
        # make sure that a Value instance is saved in the object. If something
        # else was passed to this method, update or make a new Value instance
        # out of it.
        value = None
        if isinstance(val, self.__class__.Value):
            value = val
        else:
            # update existing Value or make a new one
            if self._name in obj._declared_fields_values:
                value = obj._declared_fields_values[self._name]
                value.set(val)
            else:
                value = self.__class__.Value(obj, self, val_cached = val)
        obj._declared_fields_values[self._name] = value


    ###########################################################################
    # conversions to/from DB
    ###########################################################################

    def _construct_from_doc(self, obj, val, call_superclass = False):
        if call_superclass:
            return super()._construct_from_doc(obj, val)
        else:
            return self.__class__.Value(obj, self, val_doc = val)

    def _convert_to_doc(self, obj, val, call_superclass = False):
        if call_superclass:
            return super()._convert_to_doc(obj, val)

        # val is definitely a Value instance here
        if val._loaded:
            val_deconstructed = super()._convert_to_doc(obj, val._val_cached)
            return val._convert_to_doc(val_deconstructed)
        else:
            return val._val_doc


    ###########################################################################
    # validation
    ###########################################################################

    def validate(self, val):
        """Overrides validation, sending in our Value's stored value instead of
        the Value instance, but in the end returning the Value instance.
        """
        if val.loaded:
            validated_val = super().validate(val._val_cached)
            if validated_val != val._val_cached:
                val.set(validated_val, False)
        return val



class ReferenceField(LazyField):
    """A ReferenceField stores a reference to a Document. The DB representation
    of this is simply the referenced document's primary key. Loading the
    field's value loads the document from the database. The field's value is
    then a Document instance, of the type specified by the 'target' parameter
    to the field's constructor.

    If the referenced document does not exist in the DB, attempting to access
    it through await or load() raises a NotFoundError.
    """

    class Value(LazyField.Value):

        async def _load(self, val_doc, conn = None):
            # val_doc is the primary key for a field._target document
            return await self._field._target_resolve().load(val_doc, conn)

        def _convert_to_doc(self, val_cached):
            # primary key of the referenced document
            pkey_db_val = val_cached.__class__.pkey.\
                    _do_convert_to_doc(val_cached)
            return pkey_db_val


    def __init__(self, target, **kwargs):
        """target can be either a Document class, or a string containing the
        name of a Document class.
        """
        self._target_resolve = functools.partial(registry.resolve, target)
        super().__init__(**kwargs)


    def _validate(self, val):
        if not val.stored_in_db:
            raise ValidationError("referenced document is not "
                    "stored in the database")
        return val
