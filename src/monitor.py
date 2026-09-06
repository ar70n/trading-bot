import asyncio
from ib_async import *

import os
import socket


DEVICE_ANSWER_TIMEOUT = 1 # Tiempo de espera para recibir el estado de un dispositivo
DEVICE_ANSWER_SLEEP = 0.1 # Tiempo de espera entre comprobaciones del estado de los dispositivos
RECV_BUFFER = 1024


# ===========================================================================================
# MÉTODOS DEL CONTROLADOR
# ===========================================================================================
def process_bot_msg(msg: str, ib) -> str: # ESTA FUNCION PROCESA LOS MENSAJES DE DISCORD
    """
    Procesa un mensaje recibido por el bot y ejecuta la acción correspondiente según el comando especificado.

    Args:
        msg (str): Mensaje recibido que contiene el comando y sus argumentos.

    Returns:
        str: Respuesta generada tras procesar el comando.

    Comandos soportados:
        - 'bot_status': Obtiene el estado del bot.
        - Comando no reconocido: Devuelve un mensaje indicando que el comando no es válido.

    Excepciones:
        - Si ocurre un error se devuelve un mensaje de error.
    """
    split = msg.split(" ")

    match split[0]:
        case "bot_status":
            if ib.isConnected():
                return "El bot está conectado a TWS/Gateway."
            else:
                return "El bot NO está conectado a TWS/Gateway."
        case "shutdown":
            print("[MONITOR] Felicidades, la has liado parda.")
        case _:
            print(f"[MONITOR] Comando \"{split[0]}\" no reconocido.")
            return f"Comando \"{split[0]}\" no reconocido."
        
# ESTA FUNCION SE ENCARGA DE ENSEÑAR LA INFO POR DISCORD!
async def strategy_updates(ib):
    """
    Función que se ejecuta en un hilo separado para manejar actualizaciones de la estrategia.
    """
    try:
        while ib.isConnected():
            err = await ib.errorEvent
            reqId, errorTime, errorCode, errorString = err
            #print(f"[MONITOR] Info. Id: {reqId}, Time: {errorTime}, Code: {errorCode}, Msg: {errorString}, orderReject: {getattr(err, 'advancedOrderReject', '')}")

            #async with socket_lock:
            #    conn.send(f"[MONITOR] Info. Id: {reqId}, Time: {errorTime}, Code: {errorCode}, Msg: {errorString}, orderReject: {getattr(err, 'advancedOrderReject', '')}")
    except asyncio.CancelledError:
        print("[MONITOR] strategy_updates task cancelled.")
        pass

async def discord_updates(ib): # EN PRINCIPIO ESTA FUNCION ESTA FULL TERMINADA
    #CONEXION AL BOT
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind(('localhost', 5000))
    except socket.error as e:
        print(f"[MONITOR] Error al enlazar el socket: {e}")
        close()

    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Permitir reutilizar la dirección inmediatamente
    server_socket.listen(1)  # Escuchamos solo al bot

    #BUCLE PRINCIPAL
    
    try:
        loop = asyncio.get_running_loop()
        conn, addr = await loop.sock_accept(server_socket)
        print(f"[MONITOR] Connected to bot at {addr}")
        
        while True:
            data = await loop.sock_recv(conn, RECV_BUFFER)
            if not data:
                print("[MONITOR] Client disconnected.")
                break  # Salimos del bucle si el cliente se desconecta
            
            print(f"[MONITOR] \033[94mReceived command from bot: {data.decode('utf-8')}\033[0m")
            response = process_bot_msg(data.decode("utf-8"), ib)  # Lo procesamos y luego vamos a tener que contestarle
            print(f"[MONITOR] Response to bot: {response}")
            async with socket_lock:
                conn.send(response.encode('utf-8'))
    except asyncio.CancelledError:
        print("[MONITOR] discord_updates task cancelled.")
        pass
    return

# ============================================================================================
# MÉTODO DE CIERRE
# ============================================================================================
def close ():
    print("[MONITOR] Cerrando el socket del bot...")
    if conn is not None:
        conn.close()
    server_socket.close()

    for task in asyncio.all_tasks():
        task.cancel()

    print("[MONITOR] Programa cerrado correctamente.")
    os._exit(0)


# ============================================================================================
# MAIN
# ============================================================================================
conn = None
server_socket = None
socket_lock = asyncio.Lock()

async def monitor(ib):
    tasks = []
    tasks.append(asyncio.create_task(strategy_updates(ib))) # Strategy updates processing and sending
    tasks.append(asyncio.create_task(discord_updates(ib))) # Discord chat processing and sending
    await asyncio.gather(*tasks)
    print("[MONITOR] Programa finalizado correctamente.")
    return

