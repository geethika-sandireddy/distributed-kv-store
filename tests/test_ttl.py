import time

import pytest

from kvstore.store import Store, KeyNotFoundError


def test_set_without_ttl_never_expires():
    s = Store()
    s.set("a", 1)
    time.sleep(0.05)
    assert s.get("a") == 1


def test_key_expires_after_ttl():
    s = Store()
    s.set("a", 1, ttl_seconds=0.05)
    assert s.get("a") == 1
    time.sleep(0.08)
    with pytest.raises(KeyNotFoundError):
        s.get("a")


def test_expired_key_not_in_exists():
    s = Store()
    s.set("a", 1, ttl_seconds=0.05)
    time.sleep(0.08)
    assert not s.exists("a")


def test_expired_key_not_in_keys():
    s = Store()
    s.set("a", 1, ttl_seconds=0.05)
    s.set("b", 2)
    time.sleep(0.08)
    assert s.keys() == ["b"]


def test_expired_key_not_in_len():
    s = Store()
    s.set("a", 1, ttl_seconds=0.05)
    s.set("b", 2)
    time.sleep(0.08)
    assert len(s) == 1


def test_negative_ttl_rejected():
    s = Store()
    with pytest.raises(ValueError):
        s.set("a", 1, ttl_seconds=-1)


def test_overwrite_resets_ttl():
    s = Store()
    s.set("a", 1, ttl_seconds=0.05)
    time.sleep(0.03)
    s.set("a", 2)  # no ttl this time -> should not expire
    time.sleep(0.05)
    assert s.get("a") == 2
