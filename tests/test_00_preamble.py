import pytest

import aiorethink

"""If these tests are run at all, they must run before any other.
"""


@pytest.mark.asyncio
async def test_conn_fails_if_no_params_set():
    with pytest.raises(aiorethink.IllegalAccessError):
        cn = await aiorethink.db_conn
