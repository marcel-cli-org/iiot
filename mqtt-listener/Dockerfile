# Dockerfile für den MQTT-Listener-Pod
FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Kopiere die Requirements und installiere sie
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Kopiere das Skript
COPY mqtt_listener.py /app/mqtt_listener.py

WORKDIR /app

# Standard-Kommando: Starte das Listener-Skript
CMD ["python", "/app/mqtt-listener.py"]
