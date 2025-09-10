src
└── main
    ├── java
    │   ├── administration
    │   │   ├── AdministrationClient.java     # admin client to query informations
    │   │   ├── AdministrationServer.java     # admin server app
    │   │   ├── controller
    │   │   │   └── PlantsController.java     # REST mapping
    │   │   ├── models
    │   │   │   ├── PlantInfo.java
    │   │   │   └── PollutionReading.java
    │   │   └── service
    │   │       └── PlantService.java         # Springboot service for power plants
    │   ├── energyprovider
    │   │   └── renewableEnergyProvider.java  # renewable energy provider app
    │   ├── plant
    │   │   ├── GrpcService.java              # gRPC communications
    │   │   ├── models
    │   │   │   ├── EnergyRequestInfo.java
    │   │   │   └── PlantInfo.java
    │   │   ├── MqttHandler.java              # MQTT publish/subscribe
    │   │   ├── PlantApp.java                 # power plant app (entry point)
    │   │   ├── PlantClient.java              # REST client for admin server
    │   │   ├── PowerPlant.java               # power plant core
    │   │   ├── RingNetwork.java              # manages logical network
    │   │   └── sensors
    │   │       ├── PollutionMonitor.java     # manages sensing
    │   │       └── SensorBuffer.java         # Simulators Buffer.java implementation
    │   └── Simulators
    │       ├── Buffer.java
    │       ├── Measurement.java
    │       ├── PollutionSensor.java
    │       └── Simulator.java
    └── proto
        └── plant_comms.proto                 # gRPC service for power plants communications
