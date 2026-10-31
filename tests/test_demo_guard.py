"""El bot solo puede operar en una sesión demostrada como demo. Los puertos live se derivan de los de paper
(cada puerto live es el de paper menos uno) para no escribirlos en el código."""

from types import SimpleNamespace

import pytest

import bot
import strategy
from demo_guard import (
    DEMO_PORTS,
    NotDemoAccountError,
    assert_demo_accounts,
    assert_demo_endpoint,
    assert_order_is_demo,
)
from ib_mocks import make_mock_ib


@pytest.mark.parametrize("accounts", [["DU1234567"], ["DF1234567", "DU7654321"], ["D1234567"]])
def test_demo_accounts_pass(accounts):
    assert_demo_accounts(accounts)


REAL = "U" + "1234567"


@pytest.mark.parametrize(
    "accounts",
    [[], [""], [REAL], ["F1234567"], ["DU1234567", REAL], ["du1234567"], ["DUabc"], ["DEMO"], [" DU1234567"]],
)
def test_anything_else_is_refused(accounts):
    with pytest.raises(NotDemoAccountError):
        assert_demo_accounts(accounts)


def test_refusal_does_not_leak_account_id():
    with pytest.raises(NotDemoAccountError) as err:
        assert_demo_accounts([REAL])
    assert REAL not in str(err.value)


@pytest.mark.parametrize("port", sorted(p - 1 for p in DEMO_PORTS))
def test_live_ports_refused(port):
    with pytest.raises(NotDemoAccountError):
        assert_demo_endpoint("127.0.0.1", port)


def test_remote_host_refused():
    with pytest.raises(NotDemoAccountError):
        assert_demo_endpoint("10.0.0.5", 4002)


def test_order_for_real_account_refused():
    assert_order_is_demo(SimpleNamespace(account=""))
    assert_order_is_demo(SimpleNamespace(account="DU1234567"))
    with pytest.raises(NotDemoAccountError):
        assert_order_is_demo(SimpleNamespace(account=REAL))


async def test_session_refused_never_runs_strategy(monkeypatch):
    called = []

    async def fake_run_once(ib):
        called.append(ib)

    monkeypatch.setattr(bot, "run_once", fake_run_once)
    ib = make_mock_ib(accounts=[REAL])

    code, status = await bot.run_session(ib)

    assert (code, status) == (bot.EXIT_NOT_DEMO, "refused_not_demo")
    assert called == []
    assert ib.placed == []
    ib.disconnect.assert_called_once()


async def test_session_with_no_accounts_refused(monkeypatch):
    ib = make_mock_ib(accounts=[])
    assert (await bot.run_session(ib))[0] == bot.EXIT_NOT_DEMO


async def test_run_once_refuses_directly():
    with pytest.raises(NotDemoAccountError):
        await strategy.run_once(make_mock_ib(accounts=[REAL]))


async def test_live_port_refused_before_connecting(monkeypatch):
    monkeypatch.setattr(bot, "PORT", min(DEMO_PORTS) - 1)
    ib = make_mock_ib()
    assert (await bot.run_session(ib))[0] == bot.EXIT_NOT_DEMO
    ib.connectAsync.assert_not_called()
