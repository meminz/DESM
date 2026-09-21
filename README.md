# DESM — Distributed Energy System Management

A distributed system simulating a peer-to-peer network of thermal power plants competing to fulfill renewable energy requests. Plants self-organize into a token-ring network, run election algorithms to bid on energy requests, and report CO2 pollution data — all communicating via **gRPC**, **MQTT**, and **REST**.

## Project Requirements

The full project specification is available in [DPS_Project_2025.pdf](DPS_Project_2025.pdf).

## Architecture

```
                    ┌──────────────────────────────┐
                    │  Renewable Energy Provider   │  Publishes energy requests every 10s
                    │  (MQTT → energy/requests/*)  │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
┌──────────────────────────────────────────────────────────────┐
│                     MQTT Broker (Mosquitto)                  │
└──────┬─────────────────────────────────────────────────┬─────┘
       │ subscribe                                       │
       ▼                                                 ▼
┌─────────────┐  gRPC ring  ┌─────────────┐  gRPC ring  ┌─────────────┐
│ Power Plant │◄───────────►│ Power Plant │◄───────────►│ Power Plant │
│ (port 9001) │             │ (port 9002) │             │ (port 9003) │
└──────┬──────┘             └──────┬──────┘             └──────┬──────┘
       │ MQTT publish              │                           │
       │ pollution/data            │                           │
       ▼                           ▼                           ▼
    ┌──────────────────────────────────────────────────────────────┐
    │              Administration Server (:8080)                   │
    │          Spring Boot REST API + MQTT subscriber              │
    └──────────────────────────────┬───────────────────────────────┘
                                   │ REST
                                   ▼
                          ┌─────────────────┐
                          │ Administration  │
                          │     Client      │
                          │   (CLI menu)    │
                          └─────────────────┘
```

### Components

| Component | Description |
|---|---|
| **Power Plant** | Standalone process forming a self-organizing ring network. Competes in token-ring elections to supply energy. Simulates CO2 pollution sensing. |
| **Renewable Energy Provider** | Publishes energy purchase requests (5000–15000 kWh) via MQTT every 10 seconds. |
| **Administration Server** | Spring Boot app exposing REST endpoints for plant registration and CO2 pollution statistics. Subscribes to MQTT for live pollution data. |
| **Administration Client** | CLI tool for querying registered plants and pollution statistics. |

### How It Works

1. **Energy requests** are published by the provider to MQTT topics `energy/requests/{id}`.
2. All power plants subscribe to these topics. When a request arrives, it triggers a **token-ring election** via gRPC.
3. Each plant generates a random bid price. The election message circulates the ring — the lowest bid wins.
4. The winning plant "produces" energy (simulated delay) and the request is cleared.
5. Each plant runs a **Gaussian CO2 sensor** (sliding window of 8 readings), publishing aggregated pollution data to MQTT topic `pollution/data`.
6. The **Administration Server** collects pollution data and serves it through a REST API.

## Technologies

| Technology | Purpose |
|---|---|
| **Java 21** | Core language |
| **Spring Boot 3.2.5** | Administration server REST API |
| **gRPC 1.72.0** | Inter-plant ring network communication |
| **Protocol Buffers 4.31.0** | Serialization for gRPC messages |
| **Eclipse Paho MQTT 1.2.5** | Publish/subscribe for energy requests and pollution data |
| **Mosquitto** | MQTT broker (external dependency) |
| **Gradle** | Build system with protobuf plugin |
| **JUnit 5** | Testing |

## Prerequisites

- **Java JDK 21+**
- **Mosquitto MQTT broker** running on `localhost:1883`

Install Mosquitto:

```bash
sudo apt-add-repository ppa:mosquitto-dev/mosquitto-ppa
sudo apt-get update
sudo apt-get install mosquitto mosquitto-clients -y
mosquitto -d
```

## Build

```bash
./gradlew build
```

## Run

Start each component in a **separate terminal**.

### 1. Administration Server

```bash
./gradlew run --args='administration.AdministrationServer'
```

Runs on port **8080**.

### 2. Power Plants (one per terminal)

```bash
# Follow the prompts: enter a 4-digit port (e.g. 9001) and a unique plant ID
./gradlew run --args='plant.PlantApp'
```

Run at least one plant before starting the energy provider. The ring self-organizes as plants join.

### 3. Renewable Energy Provider

```bash
./gradlew run --args='energyprovider.renewableEnergyProvider'
```

Publishes a new energy request every 10 seconds.

### 4. Administration Client (optional)

```bash
./gradlew run --args='administration.AdministrationClient'
```

CLI menu to list plants and query CO2 pollution statistics.

## REST API

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/plants` | List all registered plants |
| `POST` | `/plants/add` | Register a new plant (returns `409` if ID exists) |
| `GET` | `/plants/get/{id}` | Get plant by ID |
| `DELETE` | `/plants/delete/{id}` | Deregister a plant |
| `GET` | `/plants/pollution/statistics?t1=...&t2=...` | Average CO2 between two timestamps |

## gRPC Service

Defined in `plant_comms.proto`:

```protobuf
service PlantCommunication {
  rpc SendGreetingsMessage(GreetingsMessage)   returns (GreetingsResponse);
  rpc SendElectionMessage(ElectionMessage)     returns (ElectionResponse);
  rpc SendFarewellMessage(FarewellMessage)     returns (FarewellResponse);
}
```

- **Greetings** — announce a new plant joining the ring.
- **Election** — circulate bids through the ring (lowest price wins).
- **Farewell** — notify plants when one is shutting down.

## Project Structure

```
src/main/java/
├── administration/
│   ├── AdministrationServer.java          # Spring Boot server + MQTT subscriber
│   ├── AdministrationClient.java          # CLI client for admin queries
│   ├── controller/
│   │   └── PlantsController.java          # REST endpoints
│   ├── service/
│   │   └── PlantService.java              # In-memory plant registry
│   └── models/
│       ├── PlantInfo.java
│       └── PollutionReading.java
├── energyprovider/
│   └── renewableEnergyProvider.java       # Publishes energy requests via MQTT
├── plant/
│   ├── PlantApp.java                      # Entry point for each power plant
│   ├── PowerPlant.java                    # Orchestrates all plant subsystems
│   ├── GrpcService.java                   # gRPC server + client for ring comms
│   ├── MqttHandler.java                   # MQTT subscribe/publish
│   ├── RingNetwork.java                   # Ring topology + election algorithm
│   ├── PlantClient.java                   # REST client for admin registration
│   ├── models/
│   │   ├── PlantInfo.java
│   │   └── EnergyRequestInfo.java
│   └── sensors/
│       ├── PollutionMonitor.java          # Sensor polling + MQTT publish
│       └── SensorBuffer.java              # Thread-safe buffer
├── Simulators/
│   ├── Simulator.java                     # Abstract sensor base class
│   ├── PollutionSensor.java               # CO2 Gaussian noise simulator
│   ├── Buffer.java                        # Buffer interface
│   └── Measurement.java                   # Sensor measurement model
└── proto/
    └── plant_comms.proto                  # gRPC service definition
```
