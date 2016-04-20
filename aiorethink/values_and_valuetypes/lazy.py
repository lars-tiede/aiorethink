from ..errors import NotLoadedError
from .base_types import TypedValueType, AnyValueType


__all__ = ["LazyValue", "LazyValueType"]


class LazyValue:
    """Unlike "regular" (not lazy) values such as strings or ints or generally
    objects that can be constructed from the DB with a non-blocking
    non-coroutine function, lazy values can be constructed from a DB
    representation with a coroutine. Lazy values are objects that offer
    functions to lazy-load or get the actual (loaded) value. The LazyValue base
    class also implements caching.

    Read usage
    ----------

    Let doc be a Document with a field called my_field which has a value that
    is an instance of LazyValue):
    - await doc.my_field loads and returns the value or returns the cached
      value if that already exists.
    - await doc.my_field.load() loads, and returns the loaded value. It also
      updates the LazyValue instance's cached value in-place.
    - doc.my_field.get() returns the cached value, or raises NotLoadedError if
      value hasn't been loaded.
    - doc.my_field.loaded is True if value has been loaded, False otherwise.

    Example::

        val = await doc.my_field
        # is the same as
        if not doc.my_field.loaded:
            await doc.my_field.load()
        val = doc.my_field.get()

    Write usage
    -----------

    `doc.my_field.set(val)` when the field value exists (this won't work when
    it's None) or generally `doc.my_field = aiorethink.lval(val)`. Both do the
    same thing. It is recommended to use the second form as that always works.

    Validation and DB<->Python conversion
    -------------------------------------

    A lazy value has a ValueType, so that (if necessary) validation and
    conversions between "lazy-loaded (DB world) value" and "lazy-loaded python
    world value" can be done by an appropriate ValueType class.

    Subclassing
    -----------

    The methods _load() and _convert_to_db() must be implemented, and
    _val_type can be overridden (it defaults to AnyValueType).

    ``_val_type`` is the instance of ValueType describing what type
    the lazy-loaded value has.
    """

    _val_type = AnyValueType()


    def __init__(self,
            val_cached = None,
            val_db = None):
        self._val_cached = val_cached
        self._val_db = val_db
        self._loaded = (val_cached != None)
        if self._loaded:
            self.set(self._val_cached) # validation and conversion to DB


    def __repr__(self):
        s = ("{cls}(loaded={self._loaded}, val_db={self._val_db}, "
                "val_cached={self._val_cached})")
        return s.format(cls = self.__class__, self = self)


    @property
    def loaded(self):
        return self._loaded


    async def load(self, conn = None):
        """Loads the value, regardless of whether it was already loaded and
        cached before. Returns the loaded value.
        """
        val_loaded = await self._load(self._val_db, conn)
        self._val_cached = self.__class__._val_type.dbval_to_pyval(
                val_loaded)
        self._loaded = True
        return self._val_cached


    async def _load(self, dbval, conn = None):
        """Override this. Return lazy-loaded value that can be passed to
        self._val_type.dbval_to_pyval to construct the python-world object.
        """
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


    def set(self, new_val):
        self.validate(new_val)
        self._val_cached = new_val
        self._loaded = True
        self._val_db = self._convert_to_db(self._val_cached)
        return self


    def _convert_to_db(self, pyval):
        """Override this. Return DB-ready value, given the python-world
        object. If you need to "deconstruct" that value first, use
        ``self._val_type.pyval_to_dbval``.
        """
        raise NotImplementedError


    def get_dbval(self):
        return self._val_db


    def validate(self, new_val):
        self.__class__._val_type.validate(new_val)
        return self



class LazyValueType(TypedValueType):
    """Implements creation, conversion and validation for LazyValues. Concrete
    subclasses must at least override ``_val_type`` with the subclass of
    LazyValue to use.
    """
    _val_instance_of = LazyValue

    # TODO maybe we can remove create_value, it's only used here and in
    # LazyDocRefValueType
    def create_value(self, val_cached = None, val_db = None):
        return self.__class__._val_instance_of(val_db = val_db,
                val_cached = val_cached)

    def dbval_to_pyval(self, dbval):
        return self.create_value(val_db = dbval)

    def pyval_to_dbval(self, pyval):
        return pyval.get_dbval()

    def _validate(self, val):
        val.validate(val._val_cached)
