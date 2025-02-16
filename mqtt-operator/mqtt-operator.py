import kopf
from kubernetes import client, config
import os
import json
import requests

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
    
    # MQTT Listener
    container_image = "registry.gitlab.com/ch-mc-b/autoshop-ms/infra/iiot/mqtt-listener:1.0.1"

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
                    # VolumeMount für /data, wenn du auf einen PersistentVolumeClaim zugreifen möchtest
                    # oder als einfachen Beispiel hier leer lassen.
                }
            ],
            "restartPolicy": "Always",
            "imagePullPolicy": "Always"
        }
    }

    # Den Pod via Kubernetes API erstellen.
    core_api = client.CoreV1Api()
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

@kopf.on.create('iiot.mc-b.ch', 'v1alpha1', 'mqttdevice')
def on_create_mqttdevice(body, spec, name, namespace, logger, **kwargs):
    logger.info(f"MQTTDevice '{name}' wurde erstellt. Lese Device, Sensoren und Aktoren ...")

    mqtt_settings = spec.get("mqttSettings", {})
    mqtt_broker_url = mqtt_settings.get("broker", "mqtt://cloud.tbz.ch:1883")
    mqtt_root_topic = mqtt_settings.get("topic", "devices")

    device_ref = spec.get("deviceRef")
    if not device_ref:
        logger.error("Kein deviceRef in MQTTDevice angegeben.")
        return {"message": f"Fehlender deviceRef in MQTTDevice '{name}'."}

    # CustomObjectsApi für clusterweite CRDs
    custom_api = client.CustomObjectsApi()

    # get_cluster_custom_object statt get_namespaced_custom_object, 
    # da Device NICHT namespaced ist
    try:
        device_cr = custom_api.get_cluster_custom_object(
            group="iiot.mc-b.ch",
            version="v1alpha1",
            plural="devices",
            name=device_ref
        )
    except client.exceptions.ApiException as e:
        logger.error(f"Device '{device_ref}' konnte nicht geladen werden: {e}")
        return {"message": f"Fehler beim Laden der Device-Ressource '{device_ref}'."}

    device_spec = device_cr.get("spec", {})
    device_topic = device_spec.get("topic", device_ref)

    # Sensoren
    sensor_topics = []
    for sensor_entry in device_spec.get("sensors", []):
        sensor_ref = sensor_entry.get("sensorRef")
        if sensor_ref:
            try:
                sensor_cr = custom_api.get_cluster_custom_object(
                    group="iiot.mc-b.ch",
                    version="v1alpha1",
                    plural="sensors",
                    name=sensor_ref
                )
                sensor_topic = sensor_cr.get("spec", {}).get("topic", sensor_ref)
                full_topic = f"{mqtt_root_topic}/{device_topic}/{sensor_topic}"
                sensor_topics.append(full_topic)
                logger.info(f"Sensor-Topic: {full_topic}")
            except client.exceptions.ApiException as e:
                logger.error(f"Sensor '{sensor_ref}' konnte nicht geladen werden: {e}")

    # Actoren
    actor_topics = []
    for actor_entry in device_spec.get("actors", []):
        actor_ref = actor_entry.get("actorRef")
        if actor_ref:
            try:
                actor_cr = custom_api.get_cluster_custom_object(
                    group="iiot.mc-b.ch",
                    version="v1alpha1",
                    plural="actors",
                    name=actor_ref
                )
                actor_topic = actor_cr.get("spec", {}).get("topic", actor_ref)
                full_topic = f"{mqtt_root_topic}/{device_topic}/{actor_topic}"
                actor_topics.append(full_topic)
                logger.info(f"Actor-Topic: {full_topic}")
            except client.exceptions.ApiException as e:
                logger.error(f"Actor '{actor_ref}' konnte nicht geladen werden: {e}")

    all_topics = sensor_topics + actor_topics

    create_mqtt_listener_pod(
        namespace=namespace, 
        mqtt_device_name=name,
        mqtt_broker_url=mqtt_broker_url,
        topics=all_topics
    )
    return {"message": f"MQTTDevice '{name}' verarbeitet, Topics: {all_topics}"}

@kopf.on.update('iiot.mc-b.ch', 'v1alpha1', 'mqttdevice')
def on_update_mqttdevice(spec, name, namespace, logger, **kwargs):
    """
    Reagiert auf Änderungen an einem MQTTDevice-Objekt.
    Beispiel: Wir könnten den vorhandenen Pod löschen/neu starten oder
    nur die geänderten Werte aktualisieren.
    """
    logger.info(f"MQTTDevice '{name}' wurde aktualisiert. Aktualisiere zweiten Pod...")

    # Als einfaches Beispiel: Vorhandenen Pod löschen und neu anlegen.
    core_api = client.CoreV1Api()

    try:
        core_api.delete_namespaced_pod(name=f"mqtt-listener-{name}", namespace=namespace)
        logger.info(f"Alter Pod mqtt-listener-{name} gelöscht.")
    except client.exceptions.ApiException as e:
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


@kopf.on.delete('iiot.mc-b.ch', 'v1alpha1', 'mqttdevice')
def on_delete_mqttdevice(name, namespace, logger, **kwargs):
    """
    Reagiert auf das Löschen eines MQTTDevice-Objekts.
    Beispiel: Lösche den zugehörigen Pod.
    """
    logger.info(f"MQTTDevice '{name}' wird gelöscht. Lösche zweiten Pod...")

    core_api = client.CoreV1Api()
    try:
        core_api.delete_namespaced_pod(name=f"mqtt-listener-{name}", namespace=namespace)
        logger.info(f"Pod mqtt-listener-{name} gelöscht.")
    except client.exceptions.ApiException as e:
        if e.status == 404:
            logger.warning("Pod war bereits weg.")
        else:
            raise

    return {"message": f"MQTTDevice '{name}' wurde erfolgreich entfernt."}
