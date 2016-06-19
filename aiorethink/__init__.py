import rethinkdb
rethinkdb.set_loop_type("asyncio")

__version__ = "0.2.2-pre"

# constants
ALL             = 0
DECLARED_ONLY   = 1
UNDECLARED_ONLY = 2

from .errors import *
from .db import *
from .values_and_valuetypes import *
from .field import *
from .document import *
