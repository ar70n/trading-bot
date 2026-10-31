"""Ciclo de vida de la sesión y códigos de salida."""

import json
import logging

import pytest

import bot
from ib_mocks import make_mock_ib


@pytest.fixture(autouse=True)
def fast(monkeypatch):
    monkeypatch.setattr(bot, "CONNECT_RETRY_S", 0)
    monkeypatch.setattr(bot, "ACK_WAIT_S", 0)


async def test_happy_path_runs_strategy_and_disconnects(monkeypatch):
    seen = []

    async def fake_run_once(ib):
        seen.append(ib)

    monkeypatch.setattr(bot, "run_once", fake_run_once)
    ib = make_mock_ib()

    assert await bot.run_session(ib) == (bot.EXIT_OK, "ok")
    assert len(seen) == 1
    ib.disconnect.assert_called_once()


async def test_connection_retries_are_bounded():
    ib = make_mock_ib()
    ib.connectAsync.side_effect = ConnectionRefusedError()

    assert await bot.run_session(ib) == (bot.EXIT_ERROR, "connection_failed")
    assert ib.connectAsync.await_count == bot.CONNECT_ATTEMPTS
    ib.disconnect.assert_called_once()


async def test_connection_recovers_after_failures(monkeypatch):
    async def fake_run_once(ib):
        return None

    monkeypatch.setattr(bot, "run_once", fake_run_once)
    ib = make_mock_ib()
    ib.connectAsync.side_effect = [TimeoutError(), ConnectionRefusedError(), None]

    assert (await bot.run_session(ib))[0] == bot.EXIT_OK
    assert ib.connectAsync.await_count == 3


async def test_strategy_crash_still_disconnects(monkeypatch):
    async def boom(ib):
        raise RuntimeError("boom")

    monkeypatch.setattr(bot, "run_once", boom)
    ib = make_mock_ib()
    with pytest.raises(RuntimeError):
        await bot.run_session(ib)
    ib.disconnect.assert_called_once()


@pytest.mark.parametrize(
    "accounts,expected_code,expected_status",
    [(["DU1234567"], bot.EXIT_OK, "ok"), (["U" + "1234567"], bot.EXIT_NOT_DEMO, "refused_not_demo")],
)
def test_main_always_writes_summary(tmp_path, monkeypatch, accounts, expected_code, expected_status):
    async def fake_run_once(ib):
        return None

    monkeypatch.setenv("TRADINGBOT_LOG_DIR", str(tmp_path))
    monkeypatch.setattr(bot, "run_once", fake_run_once)
    monkeypatch.setattr(bot, "IB", lambda: make_mock_ib(accounts=accounts))

    assert bot.main() == expected_code

    lines = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert lines[-1]["event"] == "daily.summary"
    assert (lines[-1]["status"], lines[-1]["exit_code"]) == (expected_status, expected_code)
    logging.getLogger().handlers.clear()
