import functools

from ..errors import ValidationError
from ..document import Document
from ..registry import registry
from .base_types import TypedValueType
from .lazy import LazyValueType, LazyValue


__all__ = ["LazyDocRefValueType", "LazyDocRef", "lazy_doc_ref", "lval"]


class LazyDocRef(LazyValue):
    """A LazyDocRef stores a reference to a Document. The DB representation
    of this is simply the referenced document's primary key. Lazy-loading the
    value loads the document from the database. The value is then a Document
    instance, of the type specified by the 'target' parameter to the
    ReferenceValue constructor.

    If the referenced document does not exist in the DB, attempting to access
    it through await or load() raises a NotFoundError.
    """

    class ReferencedDocumentValueType(TypedValueType):
        _val_instance_of = Document

        def _validate(self, val):
            if val != None and not val.stored_in_db:
                raise ValidationError("referenced document is not "
                        "stored in the database")

    _val_type = ReferencedDocumentValueType()

    def __init__(self, target, **kwargs):
        self._target = target
        super().__init__(**kwargs)

    async def _load(self, dbval, conn = None):
        return await self._target.load(dbval, conn)

    def _convert_to_db(self, pyval):
        return pyval.__class__.pkey._do_convert_to_doc(pyval) # TODO method name now different?



class LazyDocRefValueType(LazyValueType):
    _val_instance_of = LazyDocRef

    def __init__(self, target, **kwargs):
        """``target`` is either a Document class or the name of a Document
        class.
        """
        self._target_resolver = functools.partial(registry.resolve, target)
        super().__init__(**kwargs)

    def create_value(self, val_cached = None, val_db = None):
        return self.__class__._val_instance_of(
                val_db = val_db, val_cached = val_cached,
                target = self._target_resolver())

    def dbval_to_pyval(self, dbval):
        return self.create_value(val_db = dbval)



def lazy_doc_ref(doc):
    """Returns a LazyDocRef pointing to the given document.
    """
    if isinstance(doc, Document):
        return LazyDocRef(
                target = doc.__class__,
                val_cached = doc)
    else:
        raise TypeError("{} is not a Document".format(repr(doc)))


def lval(obj):
    """Returns a LazyValue of the appropriate type (for instance LazyDocRef)
    for the given lazy-referentiable object (for instance Document)
    """
    # TODO implement this better. I guess we need some sort of registry for
    # whatever <-> LazyValue class pairings. If we have that, then we should be
    # able to do this automatically.
    if isinstance(obj, Document):
        return lazy_doc_ref(obj)
    raise TypeError("Can't make a lazy value for a {}".format(str(obj.__class__)))
