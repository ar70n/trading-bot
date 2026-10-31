"""Guard DEMO: impide operar si la sesión no es, de forma demostrable, de una cuenta de paper trading.

Política: lista de permitidos y fallo cerrado. No hay variable de entorno ni opción para desactivarlo.
- Antes de conectar: solo host local y puertos de paper.
- Tras conectar: IB debe informar de al menos una cuenta y todas deben ser demo (prefijo D: DU..., DF...).
  El identificador de cuenta es la prueba decisiva, porque el puerto del Gateway es configurable.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

DEMO_PORTS = frozenset({4002, 7497})  # paper: IB Gateway y TWS
LOCAL_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
DEMO_ACCOUNT = re.compile(r"^D[A-Z]*\d+$")


class NotDemoAccountError(RuntimeError):
    """La sesión no está demostrada como demo. No se reintenta: requiere intervención humana."""


def assert_demo_endpoint(host: str, port: int) -> None:
    if host not in LOCAL_HOSTS:
        raise NotDemoAccountError(f"Host no permitido: {host}")
    if port not in DEMO_PORTS:
        raise NotDemoAccountError(f"Puerto no permitido: {port}; solo paper: {sorted(DEMO_PORTS)}")


def assert_demo_accounts(accounts: Iterable[str]) -> None:
    accounts = list(accounts)
    if not accounts:
        raise NotDemoAccountError("IB no ha informado de ninguna cuenta; no se puede demostrar que sea demo")
    not_demo = [a for a in accounts if not DEMO_ACCOUNT.match(a)]
    if not_demo:
        # El mensaje no incluye identificadores: acaba en logs.
        raise NotDemoAccountError(f"{len(not_demo)} de {len(accounts)} cuentas no son demo")


def assert_session_is_demo(ib: Any) -> None:
    assert_demo_accounts(ib.managedAccounts())


def assert_order_is_demo(order: Any) -> None:
    """Una orden puede dirigirse a una cuenta concreta: esa cuenta también debe ser demo."""
    account = getattr(order, "account", "")
    if account and not DEMO_ACCOUNT.match(account):
        raise NotDemoAccountError("Orden dirigida a una cuenta que no es demo")
