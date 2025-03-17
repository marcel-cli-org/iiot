import os
import sys
import paho.mqtt.client as mqtt
import json
import requests
import uuid

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Verbindung zum MQTT-Broker erfolgreich hergestellt.")
        # Abonniere alle Topics, die in TOPICS stehen
        for topic in userdata['topics']:
            client.subscribe(topic)
            print(f"Abonniere Topic: {topic}")
    else:
        print(f"Fehler bei der MQTT-Verbindung. Return-Code: {rc}")

def on_message(client, userdata, msg):
    # Nachricht empfangen
    payload_str = msg.payload.decode("utf-8")
    print(f"[{msg.topic}] -> {payload_str}")

    # Schreibe in Datei /data/<Topic>.txt
    # Ersetze evtl. verbotene Zeichen im Dateinamen.
    safe_topic = msg.topic.replace("/", "_")
    file_path = f"/data/{safe_topic}.txt"
    try:
        with open(file_path, "a") as f:
            f.write(payload_str + "\n")
    except Exception as e:
        print(f"Fehler beim Schreiben in {file_path}: {e}", file=sys.stderr)

    # Überprüfe, ob das Topic zu den relevanten Typen gehört
    last_entry = msg.topic.split("/")[-1]
    valid_types = ["shipment", "invoicing", "order"]
    if last_entry in valid_types:
        cloudevents_url = os.environ.get("CLOUDEVENTS_URL", "http://broker-ingress.knative-eventing/ms-brkr/default")
        headers = {
            "Host": "broker-ingress.knative-eventing.svc.cluster.local",
            "Ce-Id": "K-native-Broker",
            "Ce-Specversion": "1.0",
            "Ce-Type": last_entry,
            "Ce-Source": msg.topic,
            "Content-Type": "application/json"
        }
        
        # Debugging: Header ausgeben
        print("Sende HTTP POST mit folgenden Headern:")
        for key, value in headers.items():
            print(f"{key}: {value}")
        print(f"Payload: {payload_str}")
        
        try:
            resp = requests.post(cloudevents_url, headers=headers, json=json.loads(payload_str))
            resp.raise_for_status()
            print(f"CloudEvent für {last_entry} an {cloudevents_url} gesendet.")
        except Exception as e:
            print(f"Fehler beim Senden an {cloudevents_url}: {e}", file=sys.stderr)

def main():
    mqtt_broker_url = os.environ.get("MQTT_BROKER_URL", "mqtt://cloud.tbz.ch:1883")
    mqtt_device_name = os.environ.get("MQTTDEVICE_NAME", "default-device")
    unique_id = f"{mqtt_device_name}-{uuid.uuid4()}"  # Beispiel: default-device-550e8400-e29b-41d4-a716-446655440000
    print(f"MQTT-Client-ID: {unique_id}")  # Zur Überprüfung    
    
    topics_str = os.environ.get("TOPICS", "device")
    topics = topics_str.split(",") if topics_str else []

    # Aus MQTT-URL Host & Port extrahieren
    # Beispiel: "mqtt://test-broker:1883"
    if mqtt_broker_url.startswith("mqtt://"):
        mqtt_broker_url = mqtt_broker_url.replace("mqtt://", "")
    broker_parts = mqtt_broker_url.split(":")
    broker_host = broker_parts[0]
    broker_port = int(broker_parts[1]) if len(broker_parts) > 1 else 1883

    # Client mit Device-Name als Client-ID
    client = mqtt.Client(client_id=unique_id, userdata={"topics": topics})
    client.on_connect = on_connect
    client.on_message = on_message

    # Falls Username/Passwort notwendig, hier client.username_pw_set(...) aufrufen
    # client.username_pw_set("user", "password")

    # Mit dem Broker verbinden
    client.connect(broker_host, broker_port, 60)

    # Blocking loop, um Nachrichten zu verarbeiten
    client.loop_forever()

if __name__ == "__main__":
    main()