# Industrial Internet of Things

![](images/crds.png)

Custom Resources
- - -

Device – IoT Device, z.B. M5StackCore, Atom etc. beinhaltet Sensor und Actor

MQTTDevice empfängt Daten mittels des MQTT-Protokolls. HTTP und Matter Device (* in Planung).

### Custom Resources

Custom Resources erstellen

    git clone https://gitlab.com/ch-mc-b/autoshop-ms/infra/iiot.git
    cd iito
    kubectl apply -f crd
    
Sensoren, Aktoren und Devices erstellen für die [M5Stack](https://m5stack.com/) IoT Geräte    

    kubectl apply -f m5stack/sensors -f m5stack/actors -f m5stack/devices
    
Ausgabe der erstellten Geräte

    kubectl get devices,sensors,actors
    
Details zu einem Sensor

    kubectl describe sensor enviii        

### Operator Pattern und Listener erstellen
    
    cd mqtt-operator
    docker build -t registry.gitlab.com/ch-mc-b/autoshop-ms/infra/iiot/mqtt-operator:1.0.0 .
    docker push registry.gitlab.com/ch-mc-b/autoshop-ms/infra/iiot/mqtt-operator:1.0.0
    
    cd ../mqtt-listener
    docker build -t registry.gitlab.com/ch-mc-b/autoshop-ms/infra/iiot/mqtt-listener:1.0.0 .
    docker push registry.gitlab.com/ch-mc-b/autoshop-ms/infra/iiot/mqtt-listener:1.0.0

### Operator deployen
    
    cd ../mqtt-operator
    kubectl delete -f ../m5stack/mqtt       
    kubectl delete -f .
    kubectl apply -f .
    
### Testen

    kubectl apply -f ../m5stack/mqtt 
    