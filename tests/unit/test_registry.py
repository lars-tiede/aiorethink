import pytest

import aiorethink
from aiorethink.registry import registry

class Dummy:
    pass

class Dummy2:
    pass


def test_register(aiorethink_session):
    registry.register("Dummy", Dummy)
    assert "Dummy" in registry
    assert registry["Dummy"] == Dummy
    assert len(registry) == 1
    for name in registry:
        assert name == "Dummy"


def test_unregister(aiorethink_session):
    registry.register("Dummy", Dummy)
    registry.unregister("Dummy")
    assert "Dummy" not in registry
    assert len(registry) == 0


def test_double_register(aiorethink_session):
    registry.register("Dummy", Dummy)
    with pytest.raises(aiorethink.AlreadyExistsError):
        registry.register("Dummy", Dummy2)


def test_resolve(aiorethink_session):
    registry["Dummy"] = Dummy
    registry["Dummy2"] = Dummy2

    assert registry.resolve("Dummy") == Dummy
    assert registry.resolve("Dummy2") == Dummy2
    assert registry.resolve(Dummy) == Dummy
    assert registry.resolve(Dummy2) == Dummy2
    with pytest.raises(TypeError):
        registry.resolve(1)
