import collections
import functools
import inspect
import abc
import itertools

import rethinkdb as r

from .. import ALL, DECLARED_ONLY, UNDECLARED_ONLY
from ..errors import IllegalSpecError, AlreadyExistsError
from ..db import db_conn, CursorAsyncMap, _run_query
from ..field import Field, FieldAlias
from .base_types import TypedValueType

__all__ = [ "FieldContainer", "FieldContainerValueType" ]


class _MetaFieldContainer(abc.ABCMeta):

    def __init__(cls, name, bases, classdict):
        cls._map_declared_fields()
        cls._check_field_spec()

        super().__init__(name, bases, classdict)



class FieldContainer(collections.abc.MutableMapping,
        metaclass = _MetaFieldContainer):
    """A FieldContainer stores named fields. It is the base for ``Document``
    and ``SubDocument``.

    There are declared fields (i.e. instances of Field), and there are
    undeclared fields. Both can be accessed using a dict-like interface. Only
    declared fields can be accessed by attribute.

    A FieldContainer can be stored to and loaded from a RethinkDB. This way, it
    can act as a "value". An associated ValueType class,
    FieldContainerValueType, makes it possible to use FieldContainer values
    "anywhere".
    """
    _declared_fields_objects = {} # { attr name : Field instance }.
                                  # Each subclass has its own version of this.
    _dbname_to_field_name = {} # { db name : field name }
                               # Each subclass has its own version of this.


    def __init__(self, **kwargs):
        """
        Use kwargs to set fields (both declared and undeclared).
        """
        super().__init__()

        self._declared_fields_values = {} # { attr name : object }
        self._updated_fields = {}
        self._undeclared_fields = {}

        # set fields given in kwargs
        for k, v in kwargs.items():
            self[k] = v


    ###########################################################################
    # class creation (metaclass constructor calls this)
    ###########################################################################

    @classmethod
    def _map_declared_fields(cls):
        """Construct _declared_fields_objects and _dbname_to_field_name from
        Fields.
        """
        # First, inherit them from all __bases__.
        # This looks funny in FieldContainer, but is absolutely necessary for
        # all classes inheriting from FieldContainer.
        cls._declared_fields_objects = {}
        cls._dbname_to_field_name = {}
        for base in reversed(cls.__bases__):
            cls._declared_fields_objects.update(
                    getattr(base, "_declared_fields_objects", {}))
            cls._dbname_to_field_name.update(
                    getattr(base, "_dbname_to_field_name", {}))

        # then, update all fields we find in *this* class
        for name in dir(cls):
            attr = getattr(cls, name, None)

            if isinstance(attr, Field):
                # check whether the field is inherited, and if we override an
                # inherited non-Field attribute (the latter is verboten). Note
                # that both are possible when we deal with multiple
                # inheritance.
                attr_is_inherited = False
                attr_overwrites_nonfield = False
                for base in cls.__bases__:
                    if hasattr(base, name):
                        base_attr = getattr(base, name)
                        if base_attr == attr:
                            attr_is_inherited = True
                        elif not isinstance(base_attr, Field):
                            attr_overwrites_nonfield = True

                # bail out if we got an illegal field name
                if attr_overwrites_nonfield:
                    raise IllegalSpecError("Illegal field name {} "
                        "(would overwrite a non-Field attribute of same name "
                        "defined in an ancestor class).".format(name))

                # add field to our dicts if it's defined in this class (it
                # might override a Field from a parent class but that is
                # technically fine)
                if not attr_is_inherited:
                    attr.name = name
                    cls._declared_fields_objects[attr.name] = attr
                    cls._dbname_to_field_name[attr.dbname] = attr.name


    @classmethod
    def _check_field_spec(cls):
        """Subclasses can override this to check field specs for legality. The
        implementation in FieldContainer does nothing.
        """
        pass


    ###########################################################################
    # simple properties and due diligence
    ###########################################################################

    def __repr__(self):
        s = "{o.__class__.__name__}({str_rep})"
        return s.format(o = self, str_rep = str(self))

    def __str__(self):
        return str(collections.ChainMap(self._declared_fields_values,
            self._undeclared_fields))


    def mark_field_updated(self, name):
        self._updated_fields[name] = None


    @classmethod
    def has_field_attr(cls, fld_name):
        attr = getattr(cls, fld_name, None)
        if isinstance(attr, Field) or isinstance(attr, FieldAlias):
            return True
        else:
            return False



    ###########################################################################
    # DB queries and related funcs
    ###########################################################################
        
    @classmethod
    def from_cursor(cls, cursor):
        """Returns an ``aiorethink.db.CursorAsyncMap`` object, i.e. an
        asynchronous iterator that iterates over all objects in the RethinkDB
        cursor. Each object from the cursor is loaded into a FieldContainer
        instance using ``cls.from_doc``, so make sure that the query you use to
        make the cursor returns "complete" FieldContainers with all its fields
        included.

        Usage example::

            # TODO make an example for the more general FieldContainer
            conn = await aiorethink.db_conn
            all_docs_cursor = MyDocument.cq().run(conn)
            async for doc in MyDocument.from_cursor(all_docs_cursor):
                assert isinstance(doc, MyDocument) # holds
        """
        return CursorAsyncMap(cursor, functools.partial(
            cls.from_doc, stored_in_db = True)) # TODO remove stored_in_db?


    @classmethod
    async def from_query(cls, query, conn = None):
        """First executes a ReQL query, and then, depending on the query,
        returns either no object (empty result), one FieldContainer object
        (query returns an object), or an asynchronous iterator over
        FieldContainer objects (query returns a sequence). When writing your
        query, you know whether it will return a single object or a sequence
        (which might contain one object).
        
        The query may or may not already have called run():
        * if run() has been called, then the query (strictly speaking, the
          awaitable) is just awaited. This gives the caller the opportunity to
          customize the run() call.
        * if run() has not been called, then the query is run on the given
          connection (or the default connection). This is more convenient for
          the caller than the former version.

        If the query returns None, then the method returns None.

        If the query returns an object, then the method loads a FieldContainer
        object (of type ``cls``) from the result. (NB make sure that your query
        returns the whole container, i.e. all its fields). The loaded
        FieldContainer instance is returned.

        If the query returns a cursor, then the method calls
        ``cls.from_cursor`` and returns its result (i.e. an
        ``aiorethink.db.CursorAsyncMap`` object, an asynchronous iterator over
        FieldContainer objects).
        """
        res = await _run_query(query, conn)

        if res == None:
            return None
        if isinstance(res, r.Cursor):
            return cls.from_cursor(res)
        if isinstance(res, dict):
            return cls.from_doc(res, stored_in_db = True)
        raise AssertionError("Don't recognize query result. Bug!")


    @classmethod
    def from_doc(cls, doc, **kwargs):
        # instantiate FieldContainer object
        obj = cls()

        # construct field values from doc and store them in FieldContainer
        # object
        for dbkey, dbval in doc.items():
            fld_name = cls._dbname_to_field_name.get(dbkey, None)
            if fld_name != None:
                # make declared field
                fld_obj = getattr(cls, fld_name)
                fld_obj._store_from_doc(obj, dbval, mark_updated = False)
            else:
                # make undeclared field
                obj._undeclared_fields[dbkey] = dbval

        return obj


    def to_doc(self):
        """Returns suited-for-DB representation of the FieldContainer.
        """
        d = {}
        for k in self.keys(DECLARED_ONLY):
            d[getattr(self.__class__, k).dbname] = self.get_dbvalue(k)
        d.update(self._undeclared_fields)
        return d


    ###########################################################################
    # dict-like interface with access to both undeclared and declared fields,
    # by their "python world" names and their "DB world" names.
    ###########################################################################

    def __getitem__(self, fld_name):
        if self.__class__.has_field_attr(fld_name):
            return getattr(self, fld_name)
        else:
            return self._undeclared_fields[fld_name]


    def get_dbvalue(self, fld_name, default = None):
        """Returns suitable-for-DB representation (something JSON serilizable)
        of the given field. If the field is a declared field, some conversion
        might be involved, depending on the field's value type. If the field
        exists but is undeclared, the field's value is returned without any
        conversion, even if that is not json serializable. If the field does
        not exist, default is returned.
        """
        if self.has_field_attr(fld_name):
            return getattr(self.__class__, fld_name)._do_convert_to_doc(self)
        elif fld_name in self._undeclared_fields:
            return self._undeclared_fields[fld_name]
        else:
            return default


    def get_key_for_dbkey(self, dbkey):
        return self._dbname_to_field_name.get(dbkey, dbkey)
        

    def __setitem__(self, fld_name, value):
        if self.__class__.has_field_attr(fld_name):
            setattr(self, fld_name, value)
        else:
            if fld_name not in self._undeclared_fields and \
                    fld_name in self.dbkeys():
                raise AlreadyExistsError("can't create an undeclared "
                        "field named {} because a declared field uses "
                        "this name for its database representation.")
            self._undeclared_fields[fld_name] = value
            self.mark_field_updated(fld_name)


    def set_dbvalue(self, fld_name, dbvalue, mark_updated = True):
        """Sets a field's 'DB representation' value. If fld_name is not a
        declared field, this is the same as the 'python world' value, and
        set_dbvalue does the same as __setitem__. If fld_name is a declared
        field, the field's 'python world' value is constructed from dbvalue
        according to the field's value type's implementation, as if the field
        is loaded from the database.

        Note than `fld_name` refers to the field's "document field name", which
        might be different from the field's "database field name". You can
        convert a "database field name" to a "Document field name" using
        ``get_key_for_dbkey()``.
        """
        if self.has_field_attr(fld_name):
            getattr(self.__class__, fld_name).\
                    _store_from_doc(self, dbvalue, mark_updated)
        else:
            self[fld_name] = dbvalue


    def __delitem__(self, fld_name):
        if self.__class__.has_field_attr(fld_name):
            delattr(self, fld_name)
        else:
            del self._undeclared_fields[fld_name]
            self.mark_field_updated(fld_name)


    def __contains__(self, fld_name):
        if self.__class__.has_field_attr(fld_name):
            return True
        else:
            return fld_name in self._undeclared_fields


    class KeysView(collections.abc.KeysView):
        def __init__(self, keygetter, which, *args, **kwargs):
            self._keygetter = keygetter
            self._which = which
            super().__init__(*args, **kwargs)

        def __iter__(self):
            its = []
            if self._which != UNDECLARED_ONLY:
                its.append(self._keygetter())
            if self._which != DECLARED_ONLY:
                its.append(self._mapping._undeclared_fields.keys())
            return itertools.chain.from_iterable(its)

        def __len__(self):
            l = 0
            if self._which != UNDECLARED_ONLY:
                l += len(self._mapping.__class__._declared_fields_objects)
            if self._which != DECLARED_ONLY:
                l += len(self._mapping._undeclared_fields)
            return l

        def __contains__(self, x):
            return x in self.__iter__()


    def keys(self, which = ALL):
        """Returns a KeysView of field names.
        """
        return self.__class__.KeysView(
                self.__class__._declared_fields_objects.keys, which, self)


    def dbkeys(self, which = ALL):
        """Returns a KeysView of database field names.
        """
        return self.__class__.KeysView(
                self.__class__._dbname_to_field_name.keys, which, self)


    def __iter__(self):
        return self.keys().__iter__()


    def __len__(self):
        return self.len(ALL)


    def len(self, which = ALL):
        return len(self.keys(which))


    class ValuesView(KeysView):
        def __init__(self, vgetter, *args, **kwargs):
            self._vgetter = vgetter
            super().__init__(None, *args, **kwargs)

        def __iter__(self):
            return (self._vgetter(k) for k in self._mapping.keys(self._which))


    def values(self, which = ALL):
        """Returns a ValuesView of values.
        """
        return self.__class__.ValuesView(self.get, which, self)


    def dbvalues(self, which = ALL):
        """Returns a ValuesView of suited-for-DB representations of values.
        """
        return self.__class__.ValuesView(self.get_dbvalue, which, self)


    class ItemsView(ValuesView):
        def __iter__(self):
            return zip(self._mapping.keys(self._which),
                    (self._vgetter(k) for k in self._mapping.keys(self._which)))


    def items(self, which = ALL):
        return self.__class__.ItemsView(self.get, which, self)


    def dbitems(self, which = ALL):
        """Returns ItemsView of (db_key, db_value)."""
        return self.__class__.ItemsView(self.get_dbvalue, which, self)


    def clear(self, which = ALL):
        """Deletes all fields.
        """
        for fld_name in list(self.keys(which)):
            del self[fld_name]


    def copy(self, which = ALL):
        """Creates a new FieldContainer (same class as self) and (shallow)
        copies all fields. The new FieldContainer is returned.
        """
        keys_to_copy = self.keys(which)
        container = self.__class__()
        for k in keys_to_copy:
            container[k] = self[k]

        return container


    ###########################################################################
    # validation
    ###########################################################################

    def validate(self):
        """Explicitly validate the field container. aiorethink does this
        automatically when necessary (for example when an Document is saved).

        The default implementation validates all updated fields individually.

        When you override this, don't forget to call super().validate().

        The method returns self.
        """
        for fld_name in self._updated_fields.keys():
            if self.__class__.has_field_attr(fld_name):
                self.validate_field(fld_name)
        return self


    def validate_field(self, fld_name):
        """Explicitly validate the field with the given name. This happens
        automatically for most fields when they are updated. The exception to
        this are fields where aiorethink can not know when updates happen, for
        example when you change an element within a list.

        The method returns self.
        """
        fld_obj = getattr(self.__class__, fld_name, None)
        if fld_obj == None:
            raise ValueError("{} is not a validatable field".
                    format(fld_name))

        val = self.get(fld_name, fld_obj.default)
        fld_obj.validate(val)
        return self



class FieldContainerValueType(TypedValueType):
    """Subclassing: just override _val_instance_of and you should be fine.
    """
    _val_instance_of = FieldContainer

    def dbval_to_pyval(self, dbval):
        return self.__class__._val_instance_of.from_doc(dbval)

    def pyval_to_dbval(self, pyval):
        return pyval.to_doc()

    def _validate(self, val):
        val.validate()
