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

Obviously, you need a RethinkDB instance running, and you need a database and
the tables for your Document classes. aiorethink can't help you with the
former, but the latter can be achieved like so (assuming a RethinkDB runs on
localhost)::

    aiorethink.configure_db_connection(db = "my_db")
    await aiorethink.init_app_db()

Let's make a Document::

    spiderman = Hero(name = "Spiderma")

    # declared fields can be accessed by attribute or dict interface
    spiderman.name = "Spierman" # oops, typo
    spiderman["name"] = "Spiderman"

    # with the dict interface, we can use fields we don't declare
    spiderman["nickname"] = "Spidey"

    await spiderman.save()

    # if we don't declare a primary key field, RethinkDB makes an 'id' field
    print(spiderman.id)


Philosophy
----------

aiorethink aims to do the following two things very well:

* make translations between database documents and Python objects easy and
  convenient
* help with schema and validation

Other than that, aiorethink tries not to hide RethinkDB under a too thick
abstraction layer. RethinkDB's excellent Python driver, and certainly its
awesome query language, are never far removed and always easy to access. Custom
queries on document objects, and getting document objects out of vanilla
rethinkdb queries, including changefeeds, should be easy.


Features
--------

The following features are either fully or partially implemented:

* optional schema: declare fields and get serialization and validation magic
  much like you know it from other ODMs / ORMs. Or don't declare fields and
  "just use them". Or use a mix of declared and undeclared fields.
* ``dict`` interface that works the same for both declared and undeclared
  fields.
* schema for complex fields such as lists, dicts, or "sub-documents"
* all I/O is is asynchronous, done with ``async def`` / ``await`` style
  coroutines.
* lazy-loading and caching (i.e. "awaitable" fields), for example references
  to other documents.
* real-time changefeeds using asynchronous iterators (``async for``) on
  documents and document classes. aiorethink can in addition assist with Python
  object creation on just about any other changefeed.

Planned features:

* maybe explicit relations between document classes (think "has_many" etc.)
* maybe schema migrations


Status
------

aiorethink is in development. The API is not complete and not stable yet,
although the most important features are present now.
