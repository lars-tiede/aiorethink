import rethinkdb
rethinkdb.set_loop_type("asyncio")

__versioninfo__ = (0,1)

# constants
ALL             = 0
DECLARED_ONLY   = 1
UNDECLARED_ONLY = 2

from .errors import *
from .db import *
from .fields import *
from .document import *
