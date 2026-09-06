import asyncio
from ib_async import *
from monitor import monitor
from strategy import strategy, run_once

port = 4002
clientId = 1005

async def bot():
    ib = IB()
    while not ib.isConnected():
        await ib.connectAsync('127.0.0.1', port, clientId)

    tasks = []
	
    # Launch tasks
    # tasks.append(asyncio.create_task(monitor(ib))) #  task for discord communication (monitoring)
    tasks.append(asyncio.create_task(run_once(ib))) # Task for algorithmic logic and execution

    # Tengo que pensar como manejar cuando se cierra el bot
    await asyncio.gather(*tasks)
    ib.disconnect()
    

def main():
    try:
        asyncio.run(bot())
        print("[BOT] Programa finalizado correctamente.")
    except ConnectionRefusedError:
        print("[BOT] TWS/Gateway not running or API not enabled")
        main()
    except KeyboardInterrupt:
        print("[BOT] Program interrupted by user")
    except ConnectionError:
        print("[BOT] IB Gateway/TWS closed the connection")
        main()
    except Exception as e:
	    print(f"[BOT] An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()