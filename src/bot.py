"""Punto de entrada del job diario: conectar, verificar DEMO, ejecutar la estrategia, resumir y desconectar."""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Any

from ib_async import IB

from demo_guard import NotDemoAccountError, assert_demo_endpoint, assert_session_is_demo
from events import emit, emit_daily_summary, setup_logging, timed
from ib_observer import ObservedIB, reconcile_at_startup
from strategy import run_once

HOST = "127.0.0.1"
PORT = 4002
CLIENT_ID = 1005
CONNECT_ATTEMPTS = 10
CONNECT_RETRY_S = 10.0
ACK_WAIT_S = 2.0  # margen para recibir los acuses de las órdenes antes de desconectar

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_NOT_DEMO = 78  # EX_CONFIG: reintentar no lo arregla


async def connect(ib: Any) -> bool:
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        emit("connection.attempt", attempt=attempt, host=HOST, port=PORT, client_id=CLIENT_ID)
        try:
            with timed("ib.connect", attempt=attempt):
                await ib.connectAsync(HOST, PORT, CLIENT_ID)
        except (OSError, TimeoutError) as exc:
            emit("connection.failed", level=logging.WARNING, attempt=attempt, error=repr(exc))
            if attempt < CONNECT_ATTEMPTS:
                await asyncio.sleep(CONNECT_RETRY_S)
        else:
            emit("connection.connected", attempt=attempt, server_version=ib.client.serverVersion())
            return True
    emit("connection.gave_up", level=logging.ERROR, attempts=CONNECT_ATTEMPTS)
    return False


async def run_session(ib: Any) -> tuple[int, str]:
    """Devuelve (código de salida, estado para el resumen). El guard va antes de cualquier uso de la sesión."""
    try:
        assert_demo_endpoint(HOST, PORT)
        if not await connect(ib):
            return EXIT_ERROR, "connection_failed"
        assert_session_is_demo(ib)
        emit("guard.passed", accounts=ib.managedAccounts())

        observed: Any = ObservedIB(ib)  # misma interfaz que IB por delegación
        reconcile_at_startup(ib)
        await run_once(observed)
        await asyncio.sleep(ACK_WAIT_S)
        return EXIT_OK, "ok"
    except NotDemoAccountError as exc:
        emit("guard.refused", level=logging.CRITICAL, reason=str(exc))
        return EXIT_NOT_DEMO, "refused_not_demo"
    finally:
        ib.disconnect()


def main() -> int:
    setup_logging()
    emit("run.started", port=PORT, client_id=CLIENT_ID)
    code, status = EXIT_ERROR, "error"
    try:
        code, status = asyncio.run(run_session(IB()))
    except KeyboardInterrupt:
        status = "interrupted"
        emit("run.interrupted", level=logging.WARNING)
    except Exception as exc:
        emit("run.failed", level=logging.ERROR, exc_info=True, error=repr(exc))
    emit_daily_summary(status, exit_code=code)
    logging.shutdown()
    return code


if __name__ == "__main__":
    sys.exit(main())
