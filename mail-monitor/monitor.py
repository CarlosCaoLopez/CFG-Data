import docker
import requests
import os

# Credenciales desde el archivo .env
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

client = docker.from_env()

def send_telegram_alert(container_name, container_id, logs, exit_code):
    # Formateamos el mensaje
    message = (
        f"<b>ALERTA DE SERVICIO CAÍDO</b>\n\n"
        f"<b>Contenedor:</b> {container_name}\n"
        f"<b>ID:</b> {container_id}\n"
        f"<b>Exit Code:</b> {exit_code}\n\n"
        f"<b>Últimos logs:</b>\n"
        f"<pre>{logs}</pre>"
    )
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML" # Permite usar negritas y bloques de código
    }

    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        print(f"Alerta enviada a Telegram para {container_name}")
    except Exception as e:
        print(f"Error enviando alerta a Telegram: {e}")

print("Escuchando eventos de caída de contenedores...")

# Escuchar solo eventos de muerte (die) de contenedores
for event in client.events(decode=True, filters={"type": "container", "event": "die"}):
    attributes = event['Actor']['Attributes']
    container_name = attributes.get('name', 'Desconocido')
    container_id = event['Actor']['ID'][:12]
    exit_code = attributes.get('exitCode')

    # Ignorar si el contenedor se detuvo de forma normal
    if exit_code == '0':
        continue

    # Extraer los últimos 20 logs (Telegram tiene límite de caracteres, mejor 20 que 30)
    try:
        container = client.containers.get(container_id)
        logs = container.logs(tail=20).decode('utf-8', errors='ignore')
        # Escapar caracteres HTML básicos para no romper el parse_mode de Telegram
        logs = logs.replace('<', '&lt;').replace('>', '&gt;')
    except Exception as e:
        logs = f"No se pudieron obtener los logs: {e}"

    print(f"¡Alerta! Contenedor caído: {container_name} (Exit: {exit_code}). Notificando...")
    send_telegram_alert(container_name, container_id, logs, exit_code)