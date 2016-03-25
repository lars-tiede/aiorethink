from collections.abc import MutableMapping

from .errors import AlreadyExistsError

__all__ = [ "registry" ]


class DocumentRegistry(MutableMapping):

    def __init__(self):
        self._classes = {}
        super().__init__()

    def register(self, name, klass):
        if name in self._classes:
            raise AlreadyExistsError("A class named {} is already"
                    " registered.".format(name))
        self._classes[name] = klass

    def unregister(self, name):
        del self._classes[name]

    def __contains__(self, class_name):
        return class_name in self._classes

    def __getitem__(self, class_name):
        return self._classes[class_name]

    def __setitem__(self, name, klass):
        self.register(name, klass)

    def __delitem__(self, name):
        self.unregister(name)

    def __iter__(self):
        return self._classes.__iter__()

    def __len__(self):
        return len(self._classes)


    def resolve(self, class_or_name):
        """Convenience function: resolve a Document class either directly (if
        class_or_name is a class) or by lookup in the registry (if
        class_or_name is a string).
        """
        if isinstance(class_or_name, type):
            return class_or_name
        elif isinstance(class_or_name, str):
            return self[class_or_name]
        else:
            raise TypeError



registry = DocumentRegistry()
