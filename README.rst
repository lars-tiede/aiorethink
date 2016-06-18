aiorethink
==========

.. image:: https://img.shields.io/pypi/v/aiorethink.svg
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/aiorethink

.. image:: https://readthedocs.org/projects/aiorethink/badge/?version=latest
    :target: http://aiorethink.readthedocs.org/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://travis-ci.org/lars-tiede/aiorethink.svg?branch=master
    :alt: Travis CI build status
    :target: https://travis-ci.org/lars-tiede/aiorethink

.. image:: https://coveralls.io/repos/github/lars-tiede/aiorethink/badge.svg?branch=master
    :alt: coveralls status
    :target: https://coveralls.io/github/lars-tiede/aiorethink?branch=master


aiorethink is a fairly comprehensive but easy-to-use asyncio-enabled Object Document Mapper
for `RethinkDB <https://www.rethinkdb.com/>`_. It is currently in development.

Documentation: http://aiorethink.readthedocs.org (very early stages)

Source: https://github.com/lars-tiede/aiorethink


Simple example
--------------

::

    import aiorethink as ar

    class Hero(ar.Document):
        name = ar.Field(indexed = True)

That's all you need to start out with your own documents.

Obviously, you need a RethinkDB instance running, and you need a database
including the tables for your Document classes. aiorethink can't help you with
the RethinkDB instance, but the DB setup can be done like so (assuming a
RethinkDB instance runs on localhost)::

    aiorethink.configure_db_connection(db = "my_db")
    await aiorethink.init_app_db()

Let's make a document::

    spiderman = Hero(name = "Spiderma")

    # declared fields can be accessed by attribute or dict interface
    spiderman.name = "Spierman" # oops, typo
    spiderman["name"] = "Spiderman"

    # with the dict interface, we can use fields we don't declare
    spiderman["nickname"] = "Spidey"

    await spiderman.save()

    # if we don't declare a primary key field, RethinkDB makes us an 'id' field
    doc_id = spiderman.id

Load a document from the DB::

    spidey = Hero.load(doc_id) # using primary key

    spidey = Hero.from_query(  # using arbitrary query
        Hero.cq(). # "class query prefix", basically rethinkdb.table("Heros")
            get_all("Spiderman", index = "name").nth(0)
    )

Iterate over a document's RethinkDB changefeed::

    async for spidey, changed_keys, change_msg in await spidey.aiter_changes():
        if "name" in changed_keys:
            print("what, a typo again? {}?".format(spidey.name))

        # change_msg is straight from the rethinkdb changes() query


Features
--------

The following features are either fully or partially implemented:

* optional schema: declare fields in Document classes and get serialization and
  validation magic much like you know it from other ODMs / ORMs. Or don't
  declare fields and "just use them" using the dictionary interface. Or use a
  mix of declared and undeclared fields.
* schema for complex fields such as lists, dicts, or even "sub-documents" with
  named and typed fields just like documents.
* ``dict`` interface that works for both declared and undeclared fields.
* all I/O is is asynchronous, done with ``async def`` / ``await`` style
  coroutines, using asyncio.
* lazy-loading and caching (i.e. "awaitable" fields), for example references
  to other documents.
* asynchronous changefeeds using ``async for``, on documents and document
  classes. aiorethink can also assist with Python object creation on just about
  any other changefeed.

Planned features:

* maybe explicit relations between document classes (think "has_many" etc.)
* maybe schema migrations


Philosophy
----------

aiorethink aims to do the following two things very well:

* make translations between database documents and Python objects easy and
  convenient
* help with schema and validation

Other than that, aiorethink tries not to hide RethinkDB under a too thick
abstraction layer. RethinkDB's excellent Python driver, and certainly its
awesome query language, are never far removed and always easy to access. Custom
queries on document objects should be easy. Getting document objects out of
vanilla rethinkdb queries, including changefeeds, should also be easy.


Status
------

aiorethink is in development. The API is not complete and not stable yet,
although the most important features are present now.
