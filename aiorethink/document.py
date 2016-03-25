import inflection
import rethinkdb as r

from . import ALL, DECLARED_ONLY, UNDECLARED_ONLY
from .errors import IllegalSpecError, AlreadyExistsError, NotFoundError
from .registry import registry
from .validatable import Validatable
from .db import db_conn
from .fields import Field

__all__ = [ "Document" ]


class _MetaDocument(type):

    def __init__(cls, name, bases, classdict):
        parents = [b for b in bases if isinstance(b, _MetaDocument)]
        if not parents:
            super().__init__(name, bases, classdict)
            return

        super().__init__(name, bases, classdict)

        if getattr(cls, "_tablename", None) == None:
            cls._tablename = inflection.tableize(name)

        cls._map_declared_fields()

        registry.register(name, cls)


class Document(Validatable, metaclass = _MetaDocument):
    """
    Non-obvious customization:
    cls._tablename if you don't want the name of the DB table derived from the
    class name.
    cls._table_create_options dict with extra kwargs for rethinkdb.table_create
    """
    _tablename = None # set to sth in a subclass if name shuldn't be derived from class name
    _table_create_options = None # dict with extra kwargs for rethinkdb.table_create

    _declared_fields_objects = None # { attr name : Field instance }
    _dbname_to_field_name = {} # { db name : field name }
    pkey = None # alias for whichever field is the primary key. set
                # automatically for subclasses upon class creation


    def __init__(self, **kwargs):
        """Makes a Document object, but does not save it yet to the database.
        Call save() for writing to DB, or use the create() classmethod to
        create and save in one step.

        Use kwargs to set fields.

        Other optional kwargs:
        extra_validators: iterable of callable that take two arguments
            (document, document).  Think of the first argument as "self", and
            the second the document to validate. Must return the validated
            document.
        """
        self._declared_fields_values = {} # { attr name : object }
        self._updated_fields = {}
        self._undeclared_fields = {}
        self._stored_in_db = False # will be set to True if Doc is retrieved
                                   # from DB, and when saved to DB

        # set fields given in kwargs
        ## but first, salvage kwargs for parent class (yuck!)
        kwargs_parent = {
                "extra_validators": kwargs.pop("extra_validators", None)
                }
        ## ... and call parent constructor
        super().__init__(**kwargs_parent)
        ## Now make fields out of all remaining kwargs
        for k, v in kwargs.items():
            self[k] = v


    ###########################################################################
    # class creation (metaclass constructor calls this)
    ###########################################################################

    @classmethod
    def _map_declared_fields(cls):
        """Construct our Fields from class attributes
        """
        # collect all declared fields in _declared_fields_objects
        cls._declared_fields_objects = {}
        for name in dir(cls):
            attr = getattr(cls, name, None)
            if isinstance(attr, Field) and name != "pkey":
                if hasattr(Document, name):
                    raise IllegalSpecError("Illegal field name {} (would "
                    "overwrite attribute of same name defined in Document "
                    "class).".format(name))
                attr.name = name
                cls._declared_fields_objects[attr.name] = attr
                cls._dbname_to_field_name[attr.dbname] = attr.name

        # make sure we have at most one declared primary key field
        pk_name = None
        for fld_name, fld_obj in cls._declared_fields_objects.items():
            if fld_obj.primary_key:
                if pk_name != None:
                    raise IllegalSpecError("Document can't have more than 1 primary key")
                pk_name = fld_name

        # primary key: either we have an explicitly declared one, or we add a
        # field named "id"
        if pk_name == None:
            if hasattr(cls, "id"):
                raise IllegalSpecError("Need {}.id for RethinkDB's automatic "
                    "primary key attribute")
            cls.id = Field(primary_key = True)
            cls.id.name = "id"
            cls._declared_fields_objects[cls.id.name] = cls.id
            cls._dbname_to_field_name[cls.id.dbname] = cls.id.name
            cls.pkey = cls.id
        else:
            cls.pkey = getattr(cls, pk_name)


    ###########################################################################
    # simple properties and due diligence
    ###########################################################################

    def __repr__(self):
        s = "{o.__class__}({o.__class__.pkey.name}={o.pkey})"
        return s.format(o = self)

    def __str__(self):
        return str(self._declared_fields_values)


    def mark_field_updated(self, name):
        self._updated_fields[name] = None


    @classmethod
    def has_field_attr(cls, fld_name):
        return fld_name in cls._declared_fields_objects


    @property
    def stored_in_db(self):
        return self._stored_in_db


    ###########################################################################
    # DB table management
    ###########################################################################

    @classmethod
    async def table_exists(cls, conn = None):
        cn = conn or await db_conn
        db_tables = await r.table_list().run(cn)
        return cls._tablename in db_tables

    @classmethod
    async def _create_table(cls, conn = None):
        cn = conn or await db_conn
        # make sure table doesn't exist yet
        if await cls.table_exists(cn):
            raise AlreadyExistsError("table {} already exists"
                    .format(cls._tablename))

        # assemble kwargs for call to table_create
        create_args = {}
        if cls._table_create_options != None:
            create_args.update(cls._table_create_options)
        ## declare primary key field if it is not "id"
        if cls.pkey.dbname != "id":
            create_args["primary_key"] = cls.pkey.dbname

        await r.table_create(cls._tablename, **create_args).run(cn)

        # create secondary indexes for fields that have indexed == True
        for fld in cls._declared_fields_objects.values():
            if fld.indexed:
                await cls.cq().index_create(fld.dbname).run(cn)

        await cls._create_table_extras(cn)

    @classmethod
    async def _create_table_extras(cls, conn = None):
        """Override this classmethod in subclasses to take care of complex
        secondary indices and other advanced stuff that we can't (or don't)
        automatically deal with.

        The table exists at this point, so you can use cls.cq().
        """
        pass

    @classmethod
    async def _reconfigure_table(cls, conn = None):
        """In the absence of proper migrations in aiorethink, there is no nice
        way to adapt an existing database to changes to either your
        _table_create_options or your _create_table_extras().
        
        When you want to adapt an existing DB you would have to resort to do
        that either manually or by running a custom script that does the
        necessary changes. However, aiorethink allows you to do this from
        within your application code, resulting in automatic DB migration, if
        only in a pretty mad way... look at it as a very dirty but sometimes
        valuable hack.
        
        You can override this method in subclasses to reconfigure database
        tables in order to reflect your changes to _table_create_options and
        _create_table_extras change over time. This method is called every time
        aiorethink.db.init(reconfigure_db = True) is run, which might be every
        time your app is started. This has very serious implications on how you
        have to implement this method.

        Make 100% sure that this method works for every version of your
        database out there, and that it is idempotent (i.e. you can run this
        method N times and the result will always be the same). Any deviation
        from this will likely cause problems. aiorethink can not help you with
        anything here, you are entirely on your own.

        You see how this is convenient to use, but probably a bitch to write,
        compared to just running a custom script manually when necessary? And
        that it might pollute your code over time?  Again: using this function
        is asking for trouble, but sometimes it might just come in very handy.
        Use it carefully. You have been warned.

        The default implementation does nothing.
        """
        pass


    ###########################################################################
    # DB queries (load, save, delete...) and related funcs
    ###########################################################################
        
    @classmethod
    def cq(cls):
        """RethinkDB query prefix for queries on the Document's DB table.
        """
        return r.table(cls._tablename)


    @classmethod
    async def load(cls, pkey_val, conn = None):
        """Loads an object from the database, using its primary key for
        identification.
        """
        cn = conn or await db_conn
        res = await cls.cq().\
                get(pkey_val).\
                run(cn)

        if res == None:
            raise NotFoundError("no matching document")
        obj = cls.from_doc(res, True)
        return obj


    @classmethod
    def from_doc(cls, doc, stored_in_db):
        # instantiate Document object
        obj = cls()
        obj._stored_in_db = stored_in_db

        # construct field values from document and store them in Document
        # object
        for dbkey, dbval in doc.items():
            fld_name = cls._dbname_to_field_name.get(dbkey, None)
            if fld_name != None:
                # make declared field
                fld_obj = cls._declared_fields_objects[fld_name]
                fld_val = fld_obj._construct_from_doc(obj, dbval)
                getattr(cls, fld_name).__set__(obj, fld_val, mark_updated = False)
            else:
                # make undeclared field
                obj._undeclared_fields[dbkey] = dbval

        return obj


    def to_doc(self):
        """Returns suited-for-DB representation of the document.
        """
        d = {}
        for k in self.keys(DECLARED_ONLY):
            d[getattr(self.__class__, k).dbname] = self.get_dbvalue(k)
        d.update(self._undeclared_fields)
        return d


    @classmethod
    async def create(cls, **kwargs):
        """Makes a Document and saves it into the DB. Use keyword arguments for
        fields.
        """
        obj = cls(**kwargs)
        await obj.save()
        return obj


    def q(self):
        """RethinkDB query prefix for queries on the document.
        """
        pkey_dbval = self.__class__.pkey._do_convert_to_doc(self)
        return self.__class__.cq().get(pkey_dbval)


    async def save(self, conn = None):
        cn = conn or await db_conn
        if self._stored_in_db:
            return await self._update_in_db(cn)
        else:
            return await self._insert_into_db(cn)

    async def _update_in_db(self, conn = None):
        cn = conn or await db_conn
        if len(self._updated_fields) == 0:
            return

        self.validate()

        # make the dictionary for the DB query
        update_dict = {}
        for fld_name in self._updated_fields.keys():
            if self.__class__.has_field_attr(fld_name):
                # Field instance: convert field value to DB-serializable format
                fld_obj = self.__class__._declared_fields_objects[fld_name]
                db_key = fld_obj.dbname
                db_val = fld_obj._do_convert_to_doc(self)
                update_dict[db_key] = db_val
            else:
                # undeclared field: we assume that the value is serializable
                update_dict[fld_name] = self._undeclared_fields.get(fld_name,
                        None)
                # NOTE the field might have been deleted from
                # _undeclared_fields (see __delitem__). But since we can not
                # remove fields from a RethinkDB document, we have to overwrite
                # 'deleted' fields with something (here: None)...

        # update in DB
        self._updated_fields = {}
        return await self.q().\
                update(update_dict).\
                run(cn)

    async def _insert_into_db(self, conn = None):
        cn = conn or await db_conn

        self.validate()

        # make dict for the DB query
        insert_dict = {}
        for fld_name, fld_obj in self.__class__._declared_fields_objects.items():
            # don't store if primary key and not set (then DB should autogenerate)
            if fld_obj.primary_key and self[fld_name] == None:
                continue
            # convert field value to DB-serializable format
            db_key = fld_obj.dbname
            db_val = fld_obj._do_convert_to_doc(self)
            insert_dict[db_key] = db_val
        insert_dict.update(self._undeclared_fields)

        # insert document into DB
        self._updated_fields = {}
        insert_result = await self.__class__.cq().\
                insert(insert_dict).\
                run(cn)

        ## the DB might have made an automatic id for us
        if "generated_keys" in insert_result:
            new_key_dbval = insert_result["generated_keys"][0]
            self.__class__.pkey._store_from_doc(self, new_key_dbval)

        self._stored_in_db = True
        return insert_result


    async def delete(self, conn = None, **kwargs_delete):
        # NOTE referential integrity? hmm...
        cn = conn or await db_conn
        res = await self.q().delete(**kwargs_delete).run(cn)
        self._stored_in_db = False
        return res


    ###########################################################################
    # dict-like interface with access to both undeclared and declared fields,
    # by their "python world" names and their "DB world" names.
    ###########################################################################

    def __getitem__(self, fld_name):
        if fld_name == "pkey":
            fld_name = self.__class__.pkey.name

        if self.__class__.has_field_attr(fld_name):
            return getattr(self, fld_name)
        else:
            return self._undeclared_fields[fld_name]


    def get(self, fld_name, default = None):
        if fld_name == "pkey":
            fld_name = self.__class__.pkey.name

        if fld_name in self:
            return self[fld_name]
        else:
            return default


    def get_dbvalue(self, fld_name, default = None):
        """Returns suitable-for-DB representation (something JSON serilizable)
        of the given field. If the field is a declared field, some conversion
        might be involved, depending on the field type. If the field exists but
        is undeclared, the field's value is returned without any conversion,
        even if that is not json serializable. If the field does not exist,
        default is returned.
        """
        if fld_name == "pkey":
            fld_name = self.__class__.pkey.name

        if self.has_field_attr(fld_name):
            return self.__class__._declared_fields_objects[fld_name].\
                    _do_convert_to_doc(self)
        elif fld_name in self._undeclared_fields:
            return self._undeclared_fields[fld_name]
        else:
            return default


    def get_key_for_dbkey(self, dbkey, default = None):
        return self._dbname_to_field_name.get(dbkey, default)
        

    def __setitem__(self, fld_name, value):
        if fld_name == "pkey":
            fld_name = self.__class__.pkey.name

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


    def set_dbvalue(self, fld_name, dbvalue):
        """Sets a field's 'DB representation' value. If fld_name is not a
        declared field, this is the same as the 'python world' value, and
        set_dbvalue does the same as __setitem__. If fld_name is a declared
        field, the field's 'python world' value is constructed from dbvalue
        according to the field type's implementation - as if the field is
        loaded from the database.
        """
        if fld_name == "pkey":
            fld_name = self.__class__.pkey.name

        if self.has_field_attr(fld_name):
            self.__class__._declared_fields_objects[fld_name].\
                    _store_from_doc(self, dbvalue, mark_updated = True)
        else:
            self[fld_name] = dbvalue


    def __delitem__(self, fld_name):
        if fld_name == "pkey":
            fld_name = self.__class__.pkey.name

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


    def keys(self, which = ALL):
        """Returns a set of field names.
        """
        keys = set()
        if which != UNDECLARED_ONLY:
            keys.update(self.__class__._declared_fields_objects.keys())
        if which != DECLARED_ONLY:
            keys.update(self._undeclared_fields.keys())
        return keys


    def dbkeys(self, which = ALL):
        """Returns a set of database field names.
        """
        keys = set()
        if which != UNDECLARED_ONLY:
            for k in self.__class__._declared_fields_objects.keys():
                keys.add(getattr(self.__class__, k).dbname)
        if which != DECLARED_ONLY:
            keys.update(self._undeclared_fields.keys())
        return keys


    def __iter__(self):
        return self.keys().__iter__()


    def __len__(self):
        return self.len(ALL)


    def len(self, which = ALL):
        return len(self.keys(which))


    def values(self, which = ALL):
        """Returns a list of values.
        """
        return [ self[k] for k in self.keys(which) ]


    def dbvalues(self, which = ALL):
        """Returns a list of suited-for-DB representations of values.
        """
        return [ self.get_dbvalue(k) for k in self.keys(which) ]


    def items(self, which = ALL):
        d = { k: self[k] for k in self.keys(which) }
        return d.items()


    def dbitems(self, which = ALL):
        return self.to_doc().items()


    def clear(self, which = ALL):
        """Deletes all fields.
        """
        for fld_name in self.keys(which):
            del self[fld_name]


    def update(self, d = None, **kwargs):
        """Mimics dict.update(). d should be either None, a dict-like object
        with a .keys() method, or an iterable of (key, value) tuples. Any
        present kwargs are also used to set fields.
        """
        if d != None:
            if callable(getattr(d, "keys", None)):
                for k in d:
                    self[k] = d[k]
            else:
                for k, v in d: # might raise TypeError
                    self[k] = v
        for k in kwargs:
            self[k] = kwargs[k]


    def copy(self, which = ALL):
        """Creates a new Document (same class as self) and (shallow) copies all
        fields except for the primary key field, which remains unset. The new
        Document is returned.
        """
        # figure out what to copy (everything but primary key and read-only fields)
        keys_to_copy = self.keys(which)
        if which != UNDECLARED_ONLY:
            # remove primary key
            keys_to_copy.remove(self.__class__.pkey.name)

        # create new document and copy data
        doc = self.__class__()
        for k in keys_to_copy:
            doc[k] = self[k]

        return doc


    ###########################################################################
    # validation
    ###########################################################################

    def validate(self):
        """Explicitly validate the document. This happens automatically when
        the Document is saved.
        """
        # Just a shortcut to allow calling validate() without the extra val
        # argument that we don't need when validating Documents.
        return super().validate(self)


    def validate_field(self, fld_name):
        """Explicitly validate the field with the given name. This happens
        automatically for all updated fields when the Document is saved.

        The function returns the validated value. The value stored in the
        document is also updated with the validated value (your validation
        is allowed to make subtle changes to it).
        """
        fld_obj = self.__class__._declared_fields_objects.get(fld_name)
        if fld_obj == None:
            raise ValueError("{} is not a validatable field".
                    format(fld_name))

        val = self._declared_fields_values.get(fld_name, fld_obj.default)
        validated_val = fld_obj.validate(val)
        if fld_name in self._declared_fields_values:
            self._declared_fields_values[fld_name] = validated_val
        return validated_val


    def _validate(self):
        """Validates all updated fields individually.
        """
        for fld_name in self._updated_fields.keys():
            if self.__class__.has_field_attr(fld_name):
                self.validate_field(fld_name)
        return self
