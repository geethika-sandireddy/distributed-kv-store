import pytest

from kvstore.store import Store, KeyNotFoundError


def test_set_and_get():
    s = Store()
    s.set("a", 1)
    assert s.get("a") == 1


def test_get_missing_raises():
    s = Store()
    with pytest.raises(KeyNotFoundError):
        s.get("missing")


def test_get_optional_missing_returns_none():
    s = Store()
    assert s.get_optional("missing") is None


def test_overwrite():
    s = Store()
    s.set("a", 1)
    s.set("a", 2)
    assert s.get("a") == 2


def test_delete():
    s = Store()
    s.set("a", 1)
    s.delete("a")
    assert not s.exists("a")


def test_delete_missing_raises():
    s = Store()
    with pytest.raises(KeyNotFoundError):
        s.delete("missing")


def test_exists():
    s = Store()
    assert not s.exists("a")
    s.set("a", 1)
    assert s.exists("a")


def test_len():
    s = Store()
    assert len(s) == 0
    s.set("a", 1)
    s.set("b", 2)
    assert len(s) == 2


def test_keys():
    s = Store()
    s.set("a", 1)
    s.set("b", 2)
    assert set(s.keys()) == {"a", "b"}
