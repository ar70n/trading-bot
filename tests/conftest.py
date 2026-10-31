"""Aislamiento: ningún test toca la red, el Gateway, la base de datos real ni log/ del proyecto."""

from __future__ import annotations

import os
import socket
from typing import Any

import pytest

from ib_mocks import make_mock_ib


@pytest.fixture(autouse=True)
def no_git_hook_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dentro del pre-commit, git exporta GIT_DIR y GIT_INDEX_FILE, que romperían los repositorios temporales."""
    for name in list(os.environ):
        if name.startswith("GIT_"):
            monkeypatch.delenv(name)


@pytest.fixture(autouse=True)
def no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(self: socket.socket, address: Any) -> None:
        raise AssertionError(f"Un test ha intentado abrir una conexión de red: {address}")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(socket.socket, "connect_ex", refuse)


@pytest.fixture(autouse=True)
def isolated_db(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    import strategy

    path = tmp_path / "state.db"
    monkeypatch.setattr(strategy, "DB_PATH", str(path))
    strategy.init_db()
    return path


@pytest.fixture
def ib() -> Any:
    return make_mock_ib()
