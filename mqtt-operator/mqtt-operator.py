import kopf
import kubernetes
import os
import json
import requests

# Stelle sicher, dass die Kubernetes-Python-Client-Bibliothek installiert ist:
# pip install kopf kubernetes requests

#
# Hilfsfunktion: Erzeugt den zweiten Pod, der MQTT-Nachrichten empfängt.
#
def create_mqtt_listener_pod(
    namespace: str,
    mqtt_device_name: str,
    mqtt_broker_url: str,
    topics: list
):
    """
    Erzeugt ein Pod-Manifest für den "zweiten Pod", 
    der sich mit dem Broker verbindet und auf die angegebenen Topics lauscht.
    """

    # Hier beispielhaft das Docker-Image "python:3.9-slim" als Platzhalter
    # An dieser Stelle würdest du dein eigenes Image mit dem MQTT-Listener-Code verwenden.
    container_image = "python:3.9-slim"

    # Wir übergeben die nötigen Informationen als Umgebungsvariablen.
    # Du könntest alternativ ein ConfigMap/Secret verwenden.
    env_vars = [
        {"name": "MQTT_BROKER_URL", "value": mqtt_broker_url},
        {"name": "MQTTDEVICE_NAME", "value": mqtt_device_name},
        {"name": "TOPICS", "value": ",".join(topics)},
    ]

    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": f"mqtt-listener-{mqtt_device_name}",  # Eindeutiger Name
            "labels": {
                "app": "mqtt-listener",
                "mqtt-device": mqtt_device_name
            },
        },
        "spec": {
            "containers": [
                {
                    "name": "mqtt-listener",
                    "image": container_image,
                    "env": env_vars,
                    # Command/Args nur als Beispiel:
                    "command": ["python"],
                    "args": ["/app/mqtt_listener.py"],
                    # VolumeMount für /data, wenn du auf einen PersistentVolumeClaim zugreifen möchtest
                    # oder als einfachen Beispiel hier leer lassen.
                }
            ],
            "restartPolicy": "Always"
        }
    }

    # Den Pod via Kubernetes API erstellen.
    core_api = kubernetes.client.CoreV1Api()
    core_api.create_namespaced_pod(namespace=namespace, body=pod_manifest)

#
# Beispiel-Funktion: Senden der Daten als JSON mit CloudEvents-Headern
#
def send_cloudevent_data(url: str, data: dict, source: str, event_type: str):
    """
    Sende Daten per HTTP POST mit CloudEvents-Headern.
    """
    headers = {
        "Content-Type": "application/json",
        "Ce-Specversion": "1.0",
        "Ce-Type": event_type,
        "Ce-Source": source,
        "Ce-Id": "some-unique-id",  # Evtl. generieren oder aus data
    }

    response = requests.post(url, headers=headers, data=json.dumps(data))
    response.raise_for_status()  # Löst eine Exception aus, falls ein Fehler zurück kommt

#
# Operator-Funktionen
#

@kopf.on.create('example.com', 'v1', 'mqttdevices')
def on_create_mqttdevice(spec, name, namespace, logger, **kwargs):
    """
    Reagiert auf das Erstellen eines MQTTDevice-Objekts.
    """
    logger.info(f"MQTTDevice '{name}' wurde erstellt. Starte zweiten Pod...")

    # Lies die relevanten Felder aus dem spec (Beispiel: mqttBrokerUrl, devices, sensors, etc.)
    mqtt_broker_url = spec.get("mqttBrokerUrl", "mqtt://test-broker:1883")
    device_topic = spec.get("deviceTopic", "device1")
    sensors = spec.get("sensors", [])

    # Erzeuge die vollständigen MQTT-Topics, z.B. <MQTTDevice.topic>/<Device.topic>/<Sensor.topic>
    # Annahme: MQTTDevice.topic -> name (oder im CRD hinterlegt)
    # Achtung: Je nachdem, wo du das Feld im CRD speicherst, bitte anpassen.
    mqtt_device_topic = spec.get("topic", name)

    topic_list = []
    for sensor in sensors:
        sensor_topic = sensor.get("topic", "sensorX")
        full_topic = f"{mqtt_device_topic}/{device_topic}/{sensor_topic}"
        topic_list.append(full_topic)

    # Erzeuge den Pod, der die MQTT-Nachrichten empfängt
    create_mqtt_listener_pod(
        namespace=namespace,
        mqtt_device_name=name,
        mqtt_broker_url=mqtt_broker_url,
        topics=topic_list
    )

    # Hier könntest du noch Status-Informationen zurückliefern
    return {"message": f"MQTTDevice '{name}' wurde erfolgreich erstellt."}


@kopf.on.update('example.com', 'v1', 'mqttdevices')
def on_update_mqttdevice(spec, name, namespace, logger, **kwargs):
    """
    Reagiert auf Änderungen an einem MQTTDevice-Objekt.
    Beispiel: Wir könnten den vorhandenen Pod löschen/neu starten oder
    nur die geänderten Werte aktualisieren.
    """
    logger.info(f"MQTTDevice '{name}' wurde aktualisiert. Aktualisiere zweiten Pod...")

    # Als einfaches Beispiel: Vorhandenen Pod löschen und neu anlegen.
    core_api = kubernetes.client.CoreV1Api()

    try:
        core_api.delete_namespaced_pod(name=f"mqtt-listener-{name}", namespace=namespace)
        logger.info(f"Alter Pod mqtt-listener-{name} gelöscht.")
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning("Zu aktualisierender Pod war nicht vorhanden.")
        else:
            raise

    # Anschließend analog zu on_create erneut anlegen.
    mqtt_broker_url = spec.get("mqttBrokerUrl", "mqtt://test-broker:1883")
    device_topic = spec.get("deviceTopic", "device1")
    sensors = spec.get("sensors", [])
    mqtt_device_topic = spec.get("topic", name)

    topic_list = []
    for sensor in sensors:
        sensor_topic = sensor.get("topic", "sensorX")
        full_topic = f"{mqtt_device_topic}/{device_topic}/{sensor_topic}"
        topic_list.append(full_topic)

    create_mqtt_listener_pod(
        namespace=namespace,
        mqtt_device_name=name,
        mqtt_broker_url=mqtt_broker_url,
        topics=topic_list
    )

    return {"message": f"MQTTDevice '{name}' wurde erfolgreich aktualisiert."}


@kopf.on.delete('example.com', 'v1', 'mqttdevices')
def on_delete_mqttdevice(name, namespace, logger, **kwargs):
    """
    Reagiert auf das Löschen eines MQTTDevice-Objekts.
    Beispiel: Lösche den zugehörigen Pod.
    """
    logger.info(f"MQTTDevice '{name}' wird gelöscht. Lösche zweiten Pod...")

    core_api = kubernetes.client.CoreV1Api()
    try:
        core_api.delete_namespaced_pod(name=f"mqtt-listener-{name}", namespace=namespace)
        logger.info(f"Pod mqtt-listener-{name} gelöscht.")
    except kubernetes.client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning("Pod war bereits weg.")
        else:
            raise

    return {"message": f"MQTTDevice '{name}' wurde erfolgreich entfernt."}
