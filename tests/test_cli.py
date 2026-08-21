import io
import json
from argparse import Namespace
from unittest.mock import patch, MagicMock

import pytest

from kvstore import cli


def _fake_response(body: dict):
    resp = MagicMock()
    resp.read.return_value = json.dumps(body).encode("utf-8")
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    return resp


def test_cmd_set_prints_result(capsys):
    with patch("urllib.request.urlopen", return_value=_fake_response({"key": "a", "status": "ok"})):
        cli.cmd_set(Namespace(key="a", value="1", ttl=None))
    out = capsys.readouterr().out
    assert "status" in out and "ok" in out


def test_cmd_get_prints_value_only(capsys):
    with patch("urllib.request.urlopen", return_value=_fake_response({"key": "a", "value": "hello"})):
        cli.cmd_get(Namespace(key="a"))
    out = capsys.readouterr().out.strip()
    assert out == "hello"


def test_cmd_keys_prints_one_per_line(capsys):
    with patch("urllib.request.urlopen", return_value=_fake_response({"keys": ["a", "b", "c"]})):
        cli.cmd_keys(Namespace())
    out = capsys.readouterr().out.splitlines()
    assert out == ["a", "b", "c"]


def test_cmd_status_prints_dict(capsys):
    body = {"size": 5, "replayed_on_startup": 2}
    with patch("urllib.request.urlopen", return_value=_fake_response(body)):
        cli.cmd_status(Namespace())
    out = capsys.readouterr().out
    assert "5" in out and "2" in out


def test_cmd_health_prints_ok(capsys):
    with patch("urllib.request.urlopen", return_value=_fake_response({"status": "ok"})):
        cli.cmd_health(Namespace())
    out = capsys.readouterr().out
    assert "ok" in out


def test_cmd_compact_prints_result(capsys):
    with patch("urllib.request.urlopen", return_value=_fake_response({"snapshot_entries": 3, "status": "ok"})):
        cli.cmd_compact(Namespace())
    out = capsys.readouterr().out
    assert "3" in out


def test_set_with_ttl_includes_ttl_in_body():
    captured = {}

    def fake_urlopen(req):
        captured["body"] = json.loads(req.data)
        return _fake_response({"key": "a", "status": "ok"})

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        cli.cmd_set(Namespace(key="a", value="1", ttl=42.0))

    assert captured["body"]["ttl_seconds"] == 42.0


def test_http_error_exits_nonzero():
    import urllib.error

    err_body = io.BytesIO(json.dumps({"detail": "not found"}).encode("utf-8"))
    http_err = urllib.error.HTTPError(
        url="http://x", code=404, msg="Not Found", hdrs=None, fp=err_body
    )

    with patch("urllib.request.urlopen", side_effect=http_err):
        with pytest.raises(SystemExit) as exc_info:
            cli.cmd_get(Namespace(key="missing"))

    assert exc_info.value.code == 1
