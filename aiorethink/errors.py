class IllegalAccessError(Exception):
    pass

class AlreadyExistsError(IllegalAccessError):
    pass

class NotFoundError(Exception):
    pass

class IllegalSpecError(ValueError):
    pass

class ValidationError(Exception):
    pass

class StopValidation(Exception):
    pass

class NotLoadedError(Exception):
    pass
