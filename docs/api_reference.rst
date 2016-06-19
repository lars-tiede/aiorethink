.. include:: global.rst

API Reference
=============

.. automodule:: aiorethink


FieldContainer and Document
---------------------------

|FieldContainer| and |Document| store declared |Field| objects and undeclared
data.

FieldContainers can be used everywhere where declared data is used, for
instance as named fields of other FieldContainers or as items of a list.

Documents are FieldContainers with logic for saving them to, and loading them
from, a database. Unlike FieldContainers, Documents are required to be "top
level" objects, i.e. you can not store a Document object inside of another
Document object. For nesting, use FieldContainers. For referencing other
documents, use lazy references.

.. autoclass:: aiorethink.FieldContainer
    :show-inheritance:
    :members:

.. autoclass:: aiorethink.Document
    :show-inheritance:
    :members:
..    :inherited-members:
    :undoc-members:
    :special-members:
