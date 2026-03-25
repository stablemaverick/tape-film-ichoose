"""FTP client env resolution (per-supplier vs shared)."""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from app.services import supplier_fetch_service as sfs


_FTP_ENV_KEYS = (
    "MOOVIES_FTP_HOST",
    "MOOVIES_FTP_USER",
    "MOOVIES_FTP_PASSWORD",
    "MOOVIES_FTP_PORT",
    "MOOVIES_FTP_USE_TLS",
    "LASGO_FTP_HOST",
    "LASGO_FTP_USER",
    "LASGO_FTP_PASSWORD",
    "LASGO_FTP_PORT",
    "LASGO_FTP_USE_TLS",
    "FTP_HOST",
    "FTP_USER",
    "FTP_PASSWORD",
    "FTP_PORT",
    "SFTP_HOST",
    "SFTP_USER",
    "SFTP_PASSWORD",
)


@pytest.fixture(autouse=True)
def clear_ftp_env(monkeypatch: pytest.MonkeyPatch):
    for k in _FTP_ENV_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_shared_host_when_no_prefix(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FTP_HOST", "shared.example")
    monkeypatch.setenv("FTP_USER", "u")
    monkeypatch.setenv("FTP_PASSWORD", "p")
    calls: list[tuple] = []

    def fake_connect(host, user, password, port, tls_mode):
        calls.append((host, user, password, port, tls_mode))
        return MagicMock()

    monkeypatch.setattr(sfs, "_connect_ftp", fake_connect)
    sfs.build_ftp_client()
    assert calls == [("shared.example", "u", "p", 21, "auto")]


def test_moovies_prefixed_host_fallback_user_global(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MOOVIES_FTP_HOST", "moovies.example")
    monkeypatch.setenv("FTP_USER", "globaluser")
    monkeypatch.setenv("FTP_PASSWORD", "globalpass")
    calls: list[tuple] = []

    def fake_connect(host, user, password, port, tls_mode):
        calls.append((host, user, password, port, tls_mode))
        return MagicMock()

    monkeypatch.setattr(sfs, "_connect_ftp", fake_connect)
    sfs.build_ftp_client(supplier="moovies")
    assert calls[0][0] == "moovies.example"
    assert calls[0][1] == "globaluser"
    assert calls[0][2] == "globalpass"


def test_host_may_include_inline_port(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("FTP_HOST", "10.0.0.1:2121")
    monkeypatch.setenv("FTP_USER", "u")
    monkeypatch.setenv("FTP_PASSWORD", "p")
    calls: list[tuple] = []

    def fake_connect(host, user, password, port, tls_mode):
        calls.append((host, port))
        return MagicMock()

    monkeypatch.setattr(sfs, "_connect_ftp", fake_connect)
    sfs.build_ftp_client()
    assert calls[0] == ("10.0.0.1", 2121)


def test_explicit_ftp_port_overrides_inline_host_port(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MOOVIES_FTP_HOST", "10.0.0.2:21")
    monkeypatch.setenv("MOOVIES_FTP_PORT", "990")
    monkeypatch.setenv("MOOVIES_FTP_USER", "u")
    monkeypatch.setenv("MOOVIES_FTP_PASSWORD", "p")
    calls: list[tuple] = []

    def fake_connect(host, user, password, port, tls_mode):
        calls.append((host, port))
        return MagicMock()

    monkeypatch.setattr(sfs, "_connect_ftp", fake_connect)
    sfs.build_ftp_client(supplier="moovies")
    assert calls[0] == ("10.0.0.2", 990)


def test_lasgo_inherits_moovies_when_lasgo_ftp_unset(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("MOOVIES_FTP_HOST", "moovies-only.example")
    monkeypatch.setenv("MOOVIES_FTP_USER", "m_u")
    monkeypatch.setenv("MOOVIES_FTP_PASSWORD", "m_p")
    seen: list[tuple[str, str]] = []

    def fake_connect(host, user, password, port, tls_mode):
        seen.append((host, user))
        return MagicMock()

    monkeypatch.setattr(sfs, "_connect_ftp", fake_connect)
    sfs.build_ftp_client(supplier="lasgo")
    sfs.build_ftp_client(supplier="moovies")
    assert seen == [
        ("moovies-only.example", "m_u"),
        ("moovies-only.example", "m_u"),
    ]


def test_lasgo_separate_server(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LASGO_FTP_HOST", "lasgo.example")
    monkeypatch.setenv("LASGO_FTP_USER", "lasgo_u")
    monkeypatch.setenv("LASGO_FTP_PASSWORD", "lasgo_p")
    monkeypatch.setenv("MOOVIES_FTP_HOST", "moovies.example")
    monkeypatch.setenv("MOOVIES_FTP_USER", "m_u")
    monkeypatch.setenv("MOOVIES_FTP_PASSWORD", "m_p")
    seen: list[str] = []

    def fake_connect(host, user, password, port, tls_mode):
        seen.append(host)
        return MagicMock()

    monkeypatch.setattr(sfs, "_connect_ftp", fake_connect)
    sfs.build_ftp_client(supplier="lasgo")
    sfs.build_ftp_client(supplier="moovies")
    assert seen == ["lasgo.example", "moovies.example"]
