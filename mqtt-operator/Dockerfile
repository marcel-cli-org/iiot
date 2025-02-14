# Dockerfile für den Operator-Pod
FROM python:3.9-slim

# Installiere ggf. System-Pakete, die für die Python-Bibliotheken nötig sind
# (Beispiel: gcc, libssl-dev etc., falls nötig)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Kopiere die Requirements und installiere sie
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Kopiere den Operator-Code
COPY mqtt-operator.py /app/mqtt-operator.py

# Wechsle in das /app-Verzeichnis
WORKDIR /app

# Standard-Entrypoint: Kopf mit unserem Operator-Skript starten
# --standalone verhindert, dass Kopf versucht, ein eigenes Operator-Objekt zu registrieren.
ENTRYPOINT ["kopf", "run", "--standalone", "/app/mqtt-operator.py"]
