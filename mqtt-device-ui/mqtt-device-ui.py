import os
import flask
from flask import Flask, request, redirect
from kubernetes import client, config
import paho.mqtt.client as mqtt
import uuid

app = Flask(__name__)

# --------------------------------------
# A) Kubernetes-Client vorbereiten
# --------------------------------------
try:
    # Versuche die In-Cluster-Config (Operator / Pod in K8s)
    config.load_incluster_config()
except:
    # Fallback: Lokal (z. B. ~/.kube/config)
    config.load_kube_config()

custom_api = client.CustomObjectsApi()

# --------------------------------------
# B) MQTT-Client vorbereiten
# --------------------------------------
mqtt_broker_url = os.environ.get("MQTT_BROKER_URL", "mqtt://localhost:1883")

# Aus MQTT-URL Host & Port extrahieren (z. B. "mqtt://test-broker:1883")
if mqtt_broker_url.startswith("mqtt://"):
    mqtt_broker_url = mqtt_broker_url.replace("mqtt://", "")
broker_parts = mqtt_broker_url.split(":")
broker_host = broker_parts[0]
broker_port = int(broker_parts[1]) if len(broker_parts) > 1 else 1883

# Generiere eine eindeutige UUID als Client-ID
unique_id = f"mqtt-device-ui-{uuid.uuid4()}"  # Beispiel: mqtt-550e8400-e29b-41d4-a716-446655440000
print(f"MQTT-Client-ID: {unique_id}")  # Zur Überprüfung

mqtt_client = mqtt.Client(client_id=unique_id)
# Falls nötig: mqtt_client.username_pw_set("user", "password")
mqtt_client.connect(broker_host, broker_port, 60)
mqtt_client.loop_start()  # Startet den Hintergrund-Thread für MQTT

# --------------------------------------
# C) Flask-Route: /
#    Zeigt alle mqtt_device_uis, zugehörige Device/Sensors/Actors
# --------------------------------------
@app.route("/")
def index():
    # Namespace per Query-Param, Default 'default'
    namespace = request.args.get("namespace", "default")

    # Einfaches HTML + Minimal-CSS
    html = """
    <html>
    <head>
        <title>mqtt-device-ui UI</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
            }
            h1, h2 {
                color: #333;
            }
            .device-card {
                border: 1px solid #ccc;
                border-radius: 6px;
                padding: 10px;
                margin-bottom: 20px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin: 10px 0;
            }
            th, td {
                text-align: left;
                padding: 8px;
                border-bottom: 1px solid #ddd;
            }
            th {
                background-color: #f9f9f9;
            }
            .sensor-button {
                background-color: #0284c7;
                color: white;
                border: none;
                padding: 6px 12px;
                cursor: pointer;
                border-radius: 4px;
            }
            .sensor-button:hover {
                background-color: #0369a1;
            }
            .error {
                color: red;
            }
        </style>
    </head>
    <body>
        <h1>mqtt-device-ui UI</h1>
    """

    # 1) Alle mqtt_device_uis im Namespace laden
    try:
        mqtt_device_uis = custom_api.list_namespaced_custom_object(
            group="iiot.mc-b.ch",
            version="v1alpha1",
            namespace=namespace,
            plural="mqttdevices",
        )
        items = mqtt_device_uis.get("items", [])
    except Exception as e:
        html += f"<p class='error'>Fehler beim Laden der mqttdevices: {e}</p>"
        html += "</body></html>"
        return html

    if not items:
        html += f"<p>Keine mqttdevices im Namespace '{namespace}' gefunden.</p>"
        html += "</body></html>"
        return html

    # 2) Für jedes mqttdevices: ausgeben + zugehörige Device-Infos
    for mqttdev in items:
        mqttdev_name = mqttdev["metadata"]["name"]
        spec = mqttdev.get("spec", {})

        # mqttSettings
        mqtt_settings = spec.get("mqttSettings", {})
        mqtt_root_topic = mqtt_settings.get("topic", "devices")
        broker = mqtt_settings.get("broker", "n/a")

        # deviceRef: link zur Device-Ressource (clusterweit)
        device_ref = spec.get("deviceRef", None)

        html += f"<div class='device-card'>"
        html += f"<h2>mqtt-device-ui: {mqttdev_name}</h2>"
        html += "<table>"
        html += f"<tr><th>deviceRef</th><td>{device_ref}</td></tr>"
        html += f"<tr><th>Broker</th><td>{broker}</td></tr>"
        html += f"<tr><th>Root Topic</th><td>{mqtt_root_topic}</td></tr>"
        html += "</table>"

        # --- Device laden (clusterweit) ---
        if not device_ref:
            html += "<p class='error'>Kein deviceRef angegeben.</p>"
            html += "</div>"
            continue

        try:
            device_cr = custom_api.get_cluster_custom_object(
                group="iiot.mc-b.ch",
                version="v1alpha1",
                plural="devices",  # Device ist clusterweit
                name=device_ref
            )
            device_spec = device_cr.get("spec", {})
            device_topic = device_spec.get("topic", device_ref)

            html += f"<h3>Device: {device_ref} (topic={device_topic})</h3>"

            # --- Sensors bearbeiten ---
            sensors = device_spec.get("sensors", [])
            if sensors:
                html += "<table>"
                html += "<tr><th>Sensor Ref</th><th>Full Topic</th><th>Aktion</th></tr>"
                for sensor_entry in sensors:
                    sensor_ref = sensor_entry.get("sensorRef")
                    if not sensor_ref:
                        continue

                    try:
                        sensor_cr = custom_api.get_cluster_custom_object(
                            group="iiot.mc-b.ch",
                            version="v1alpha1",
                            plural="sensors",
                            name=sensor_ref
                        )
                        sensor_spec = sensor_cr.get("spec", {})
                        sensor_topic = sensor_spec.get("topic", sensor_ref)

                        # Baue das *volle* Topic zusammen:
                        # z.B. devices/m5stackcore/env
                        full_topic = f"{mqtt_root_topic}/{device_topic}/{sensor_topic}"

                        # Button: POST /publish
                        # => übergibt "topic"=full_topic & "sensorRef"=sensor_ref
                        html += "<tr>"
                        html += f"<td>{sensor_ref}</td>"
                        html += f"<td>{full_topic}</td>"
                        html += f"""
                            <td>
                              <form action="/publish" method="POST" style="display:inline;">
                                  <input type="hidden" name="topic" value="{full_topic}">
                                  <input type="hidden" name="sensorRef" value="{sensor_ref}">
                                  <button type="submit" class="sensor-button">Send Data</button>
                              </form>
                            </td>
                        """
                        html += "</tr>"

                    except Exception as e_s:
                        html += f"<tr><td colspan='3' class='error'>Fehler beim Laden von Sensor {sensor_ref}: {e_s}</td></tr>"
                html += "</table>"
            else:
                html += "<p>Keine Sensoren vorhanden.</p>"

            # --- Actors bearbeiten ---
            actors = device_spec.get("actors", [])
            if actors:
                html += "<h4>Actors</h4>"
                html += "<table>"
                html += "<tr><th>Actor Ref</th><th>Topic</th></tr>"
                for actor_entry in actors:
                    actor_ref = actor_entry.get("actorRef")
                    if not actor_ref:
                        continue
                    try:
                        actor_cr = custom_api.get_cluster_custom_object(
                            group="iiot.mc-b.ch",
                            version="v1alpha1",
                            plural="actors",
                            name=actor_ref
                        )
                        actor_spec = actor_cr.get("spec", {})
                        actor_topic = actor_spec.get("topic", actor_ref)
                        # Hier könnte man auch das Full-Topic bauen (wenn relevant)
                        # full_topic = f"{mqtt_root_topic}/{device_topic}/{actor_topic}"

                        html += f"<tr><td>{actor_ref}</td><td>{actor_topic}</td></tr>"
                    except Exception as e_a:
                        html += f"<tr><td colspan='2' class='error'>Fehler beim Laden von Actor {actor_ref}: {e_a}</td></tr>"
                html += "</table>"
            html += "</div>"  # Ende device-card
        except Exception as e_d:
            html += f"<p class='error'>Fehler beim Laden der Device-Ressource '{device_ref}': {e_d}</p>"
            html += "</div>"

    html += "</body></html>"
    return html

# --------------------------------------
# D) Publish-Route: /publish
#    - Nimmt full_topic & sensorRef entgegen
#    - Publiziert Beispiel-Daten
# --------------------------------------
@app.route("/publish", methods=["POST"])
def publish():
    """
    POST-Felder: topic=..., sensorRef=...
    Sende ENV- oder RFID-Daten je nach sensorRef.
    """
    full_topic = request.form.get("topic", "")
    sensor_ref = request.form.get("sensorRef", "")

    if "env" in sensor_ref.lower():
        # ENV-Beispiel: 0xBC;25.0,50.0,900
        payload = "0xBC;25.0,50.0,900"
    elif "rfid" in sensor_ref.lower():
        # RFID-Beispiel: RFID=12345
        payload = "RFID=12345"
    else:
        # Fallback
        payload = f"TestMsg for {sensor_ref}"

    try:
        mqtt_client.publish(full_topic, payload)
        msg = f"Data '{payload}' auf Topic '{full_topic}' gesendet."
        print(msg)
        # Zurück zur Hauptseite
        return redirect("/")
    except Exception as e:
        return f"Fehler beim Publish auf '{full_topic}': {e}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=True)
