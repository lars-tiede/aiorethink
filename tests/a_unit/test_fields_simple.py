import pytest

import aiorethink

def test_string(aiorethink_session):
    class Doc(aiorethink.Document):
        f1 = aiorethink.StringField()
        f2 = aiorethink.StringField(max_length = 3)
        f3 = aiorethink.StringField(required = True, regex = "^[a-z]+$")
        f4 = aiorethink.StringField(max_length = 3, regex = "^[a-z]+$")
    d = Doc()

    assert d.validate_field("f1") == None
    d.f1 = "1"
    assert d.validate_field("f1") == "1"
    d.f1 = 1
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f1")

    assert d.validate_field("f2") == None
    d.f2 = "abc"
    assert d.validate_field("f2") == "abc"
    d.f2 = "abcd"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f2")

    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f3")
    d.f3 = "abcde"
    assert d.validate_field("f3") == "abcde"
    d.f3 = "123"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f3")

    d.validate_field("f4")
    d.f4 = "abc"
    assert d.validate_field("f4") == "abc"
    d.f4 = "abcd"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f4")
    d.f4 = "123"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f4")
    d.f4 = "12345"
    with pytest.raises(aiorethink.ValidationError):
        d.validate_field("f4")


def test_illegal_string_spec(aiorethink_session):
    with pytest.raises(aiorethink.IllegalSpecError):
        class Doc(aiorethink.Document):
            f1 = aiorethink.StringField(max_length = 3, default = "abcde")
