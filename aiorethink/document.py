import collections
import functools
import inspect

import inflection
import rethinkdb as r

from . import ALL, DECLARED_ONLY, UNDECLARED_ONLY
from .errors import IllegalSpecError, AlreadyExistsError, NotFoundError
from .registry import registry
from .db import db_conn, CursorAsyncIterator, CursorAsyncMap, ChangesAsyncMap,\
            _run_query
from .field import Field, FieldAlias
from .values_and_valuetypes.field_container import FieldContainer, _MetaFieldContainer

__all__ = [ "Document" ]


class _MetaDocument(_MetaFieldContainer):

    def __init__(cls, name, bases, classdict):
        cls._tablename = cls._get_tablename()

        # make sure that the following runs only for subclasses of Document.
        # There's no really nice way to do this AFAIK, because 'Document' is
        # not known yet when this is called. The best I could come up with is
        # this:
        if bases != (FieldContainer,):
            registry.register(name, cls)

        super().__init__(name, bases, classdict)



class Document(FieldContainer, metaclass = _MetaDocument):
    """
    Non-obvious customization:
    cls._table_create_options dict with extra kwargs for rethinkdb.table_create
    """
    _tablename = None # customize with _get_tablename - don't set this attr
    _table_create_options = None # dict with extra kwargs for rethinkdb.table_create


    def __init__(self, **kwargs):
        """Makes a Document object, but does not save it yet to the database.
        Call save() for writing to DB, or use the create() classmethod to
        create and save in one step.

        Use kwargs to set fields.
        """
        super().__init__(**kwargs)

        self._stored_in_db = False # will be set to True if Doc is retrieved
                                   # from DB, and when saved to DB


    ###########################################################################
    # class creation (metaclass constructor calls this)
    ###########################################################################

    @classmethod
    def _get_tablename(cls):
        """Override this in subclasses if you want anything other than a table
        name automatically derived from the class name using
        inflection.tableize().

        Mind the dangers of further subclassing, and of using the same table
        for different Document classes.
        """
        return inflection.tableize(cls.__name__)


    @classmethod
    def _check_field_spec(cls):
        # make sure that this runs only for subclasses of Document. There's no
        # really nice way to do this AFAIK, because 'Document' is not known yet
        # when this is called. The best I could come up with is this:
        if cls.__bases__ == (FieldContainer,):
            return

        # make sure that we have at most one primary key field
        pk_name = None
        for fld_name, fld_obj in cls._declared_fields_objects.items():
            if fld_obj.primary_key:
                if pk_name != None:
                    raise IllegalSpecError("Document can't have more than 1 "
                        "primary key")
                pk_name = fld_name

        # primary key: either we have an explicitly declared one, or we add a
        # primary key field named "id"
        if pk_name == None:
            if hasattr(cls, "id"):
                raise IllegalSpecError("Need {}.id for RethinkDB's automatic "
                    "primary key attribute")
            cls.id = Field(primary_key = True)
            cls.id.name = "id"
            cls._declared_fields_objects[cls.id.name] = cls.id
            cls._dbname_to_field_name[cls.id.dbname] = cls.id.name
            pk_name = cls.id.name

        # add cls.pkey alias, pointing to the primary key field
        if getattr(cls, "pkey", None) and not isinstance(cls.pkey, FieldAlias):
            raise IllegalSpecError("'pkey' attribute is reserved for a "
                    "FieldAlias to the primary key field.")
        cls.pkey = FieldAlias(getattr(cls, pk_name))


    ###########################################################################
    # simple properties and due diligence
    ###########################################################################

    def __repr__(self):
        s = "{o.__class__.__name__}({o.__class__.pkey.name}={o.pkey})"
        return s.format(o = self)


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
    async def aiter_table_changes(cls, changes_query = None, conn = None):
        """Executes `changes_query` and returns an asynchronous iterator (a
        ``ChangesAsyncMap``) that yields (document object, changefeed message)
        tuples.

        If `changes_query` is None, `cls.cq().changes(include_types = True)`
        will be used, so the iterator will yield all new or changed documents
        in cls's table, and changefeed messages will have a "type" attribute
        giving you more information about what kind of change happened.

        If you sepcify `changes_query`, the query must return one complete
        document in new_val on each message. So don't use pluck() or something
        to that effect in your query.
        
        The query may or may not already have called run():
        * if run() has been called, then the query (strictly speaking, the
          awaitable) is just awaited. This gives the caller the opportunity to
          customize the run() call.
        * if run() has not been called, then the query is run on the given
          connection (or the default connection). This is more convenient for
          the caller than the former version.
        """
        if changes_query == None:
            changes_query = cls.cq().changes(include_types = True)

        feed = await _run_query(changes_query)
        mapper = functools.partial(cls.from_doc, stored_in_db = True) 

        return ChangesAsyncMap(feed, mapper)


    @classmethod
    async def load(cls, pkey_val, conn = None):
        """Loads an object from the database, using its primary key for
        identification.
        """
        obj = await cls.from_query(
                cls.cq().get(pkey_val),
                conn)

        if obj == None:
            raise NotFoundError("no matching document")
        return obj


    @classmethod
    def from_doc(cls, doc, stored_in_db, **kwargs):
        obj = super().from_doc(doc, **kwargs)
        obj._stored_in_db = stored_in_db
        return obj


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
                fld_obj = getattr(self.__class__, fld_name)
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
                # TODO: make replace() query then

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
            if fld_obj.primary_key and self.get(fld_name, None) == None:
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
        cn = conn or await db_conn
        res = await self.q().delete(**kwargs_delete).run(cn)
        self._stored_in_db = False
        return res


    def copy(self, which = ALL):
        """Creates a new Document (same class as self) and (shallow) copies all
        fields except for the primary key field, which remains unset. The new
        Document is returned.
        """
        doc = super().copy(which)
        del doc[self.__class__.pkey.name]
        return doc


    ###########################################################################
    # Point changefeeds (changefeeds on a single document object)
    ###########################################################################
        
    class ChangesAsyncIterator(collections.abc.AsyncIterator):
        def __init__(self, doc, conn = None):
            self.doc = doc
            self.conn = conn


        async def __aiter__(self):
            query = self.doc.q().changes(include_initial = True,
                    include_types = True)
            self.cursor = await _run_query(query, self.conn)
            return self


        async def __anext__(self):
            while True:
                try:
                    msg = await self.cursor.next()
                except r.ReqlCursorEmpty:
                    raise StopAsyncIteration

                if "new_val" not in msg:
                    continue

                doc = self.doc

                # update doc and return changed fields
                if msg["new_val"] == None:
                    doc._stored_in_db = False
                    return doc, None, msg
                else:
                    changed_fields = {k: v for k, v in msg["new_val"].items()
                            if k not in doc or v != doc.get_dbvalue(k)}

                    if not changed_fields:
                        continue

                    for k, v in changed_fields.items():
                        doc_key = doc.get_key_for_dbkey(k)
                        doc.set_dbvalue(doc_key, v, mark_updated = False)

                    return doc, list(changed_fields.keys()), msg


    async def aiter_changes(self, conn = None):
        """Note: be careful what you wish for. The document object is updated
        in place when you iterate. Unsaved changes to it might then be
        overwritten.
        """
        return self.__class__.ChangesAsyncIterator(self, conn)
