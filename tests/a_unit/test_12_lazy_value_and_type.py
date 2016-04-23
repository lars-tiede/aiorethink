import pytest

import aiorethink as ar


@pytest.fixture
def simple_lazy_type():
    class SimpleLazyValue(ar.LazyValue):
        _val_type = ar.StringValueType(regex="^[a-z]*$")

        async def _load(self, dbval, conn = None):
            return dbval.lower()

        def _convert_to_db(self, pyval):
            return pyval.upper()

    class SimpleLazyValueType(ar.LazyValueType):
        _val_instance_of = SimpleLazyValue

    return SimpleLazyValueType


@pytest.mark.asyncio
async def test_simple_lazy_value(simple_lazy_type):
    vt = simple_lazy_type()

    val = vt.dbval_to_pyval("HELLO")
    assert isinstance(val, ar.LazyValue)
    assert not val.loaded
    v = await val.load()
    assert v == "hello"
    assert val.loaded

    assert val.get() == "hello"
    assert val.get_dbval() == "HELLO"

    with pytest.raises(ar.ValidationError):
        val.set("Hello")
    assert val.get() == "hello"

    assert val.set("world") == val
    assert val.get_dbval() == "WORLD"

    assert vt.pyval_to_dbval(val) == "WORLD"


@pytest.mark.asyncio
async def test_value_inits(simple_lazy_type):
    vt = simple_lazy_type()

    v0 = vt.create_value()
    vd = vt.create_value(val_db = "HELLO")
    vp = vt.create_value(val_cached = "hello")
    v2 = vt.create_value(val_db = "HELLO", val_cached = "hello")

    vs = [v0, vd, vp, v2]
    vds = [vd, v2]
    vps = [vp, v2]

    for v in vs:
        assert "val_cached" in repr(v)
        assert vt.validate(v) == vt

    assert not vd.loaded
    with pytest.raises(ar.NotLoadedError):
        vd.get()
    assert await vd == "hello"

    for v in vps:
        assert v.loaded
        assert v.get() == "hello"
        assert await v == "hello"

    for v in vds + vps:
        assert v.get_dbval() == "HELLO"

    for v in vs:
        assert v.set("world") == v
        assert v.get() == "world"
        assert v.get_dbval() == "WORLD"
        assert vt.validate(v) == vt

