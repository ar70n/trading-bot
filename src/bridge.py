"""
Bridge.py
DESCRIPCIÓN:
    Este script implementa un bot de Discord que actúa como puente entre el usuario y un monitor de dispositivos.
    El bot permite al usuario enviar comandos al monitor y recibir respuestas.
    El bot se conecta a un socket TCP para comunicarse con el monitor.
    El bot utiliza la librería discord.py para interactuar con la API de Discord.
"""
# This bot requires the 'message_content' intent.
from multiprocessing import Process
from time import sleep
import discord
import os
from dotenv import load_dotenv
import sys
import socket
from bot import main

RECV_BUFFER = 1024

# Definimos el cliente de discord
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Eventos
@client.event
async def on_ready():
    print(f'[BRIDGE] We have logged in as {client.user}')

# Hay que hacer los comandos que puede recibir el bot y que acciones hacen esos comandos
# El bot tiene que poder imprimir el estado del bot, de los tradeos, de los gráficos, etc.

"""
FUNCION: on_message(message)
ARGS_IN:
    - message: Mensaje recibido en el canal de Discord 
DESCRIPCIÓN: Hace un chequeo superficial del mensaje recibido y si es válido lo envía al monitor.
ARGS_OUT: None
"""
@client.event
async def on_message(message): # PREPROCESA LOS MENSAJES
    if message.author == client.user:
        return
    
    if message.content.startswith('help'):
        await message.channel.send(
        """Bienvenido al bot de la casa inteligente!
Este bot te permite controlar los dispositivos de tu casa inteligente y consultar su estado.
Puedes usar los siguientes comandos:
- `add_device <id dispositivo>`: Registra un nuevo dispositivo.
- `get_device <id dispositivo>`: Consulta el estado de los dispositivos. Si usas `all`, muestra el de todos.
- `set_device <id> <new state>`: Establece el estado de un dispositivo.
- `remove_device <id>`: Elimina un dispositivo registrado.
- `add_rule si <id dispositivo modificado> <operador comparación> <estado a comparar> entonces <id dispositivo a modificar> <nuevo estado>`: Añade una regla.
- `edit_rule <rule_id> si <id dispositivo modificado> <operador comparación> <estado a comparar> entonces <id dispositivo a modificar> <nuevo estado>`: Edita una regla.
- `delete_rule <rule_id>`: Elimina una regla.
- `list_rules`: Lista las reglas.
- `help`: Muestra esta ayuda."""
            )
        return
    
    if message.content.startswith('shutdown'):
        print("[BRIDGE] Saliendo del programa correctamente.")
        await message.channel.send("Stay broke lil nigga")
        client_socket.close()
        await client.close()
        return
    # Si el mensaje no empieza por uno de los comandos, no lo procesamos
    elif not message.content.startswith(('bot_status')):
        await message.channel.send("Comando no reconocido. Usa `help` para ver la lista de comandos.")
        return

    client_socket.send(message.content.encode())
    print(f"[BRIDGE] Mensaje enviado al monitor: \"{message.content}\"")
    response = client_socket.recv(RECV_BUFFER).decode()
    print(f"[BRIDGE] Respuesta del monitor: \"{response}\"")
    if len(response) > 0:
        await message.channel.send(response)

        

"""
FUNCION: parse_args()
ARGS_IN: None
DESCRIPCIÓN: Método para parsear los argumentos de la línea de comandos.Puede recibir opcionalmente --host y --port
ARGS_OUT: args: Objeto con los argumentos parseados.
"""
def parse_args():
    import argparse
    parser = argparse.ArgumentParser(description='monitor de dispositivos')
    parser.add_argument('--host', type=str, default='localhost', help='Host del monitor')
    parser.add_argument('--port', type=int, default=5000, help='Puerto del monitor')
    return parser.parse_args()  

# Método principal
if __name__ == "__main__":
    try:
        args = parse_args()
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        # Launch main in a separate thread
        p = Process(target=main)
        p.start()
        sleep(5)
        client_socket.connect((args.host, args.port))
        #Conectar a discord
        load_dotenv()
        client.run(os.getenv('DISCORD_TOKEN'))
        print("[BRIDGE] Programa finalizado correctamente.")
    except socket.error as e:
        print("[BRIDGE] No se pudo conectar al socket del monitor. Asegúrate de que el monitor está en ejecución.")
        client_socket.close()
        sys.exit(1)
    except KeyboardInterrupt:
        print("[BRIDGE] Saliendo del programa.")
        client_socket.close()
        sys.exit(0)
    except Exception as e:
        print(f"[BRIDGE] Ha ocurrido un error inesperado: {e}")
        client_socket.close()
        sys.exit(1)