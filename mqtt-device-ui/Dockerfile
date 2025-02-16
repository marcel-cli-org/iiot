FROM python:3.9-slim

# Arbeitsverzeichnis im Container
WORKDIR /app

# requirements installieren
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App-Code kopieren
COPY mqtt-device-ui.py .

# Port in der Container-Umgebung
ENV PORT=8080
EXPOSE 8080

# Startbefehl
CMD ["python", "mqtt-device-ui.py"]