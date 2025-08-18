package plant;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.eclipse.paho.client.mqttv3.*;
import org.json.JSONArray;
import org.json.JSONObject;


import org.springframework.web.client.RestTemplate;

import Simulators.Measurement;
import Simulators.PollutionSensor;

public class PowerPlant {
    private String plantId;
    private String listeningAddress;
    private String adminServer;
    private volatile boolean isShuttingDown;

    // sensing
    private PollutionSensor pollutionSensor;
    private List<Measurement> slidingWindow = new ArrayList<>();
    private List<Double> pendingAverages = new ArrayList<>();
    private Thread sensorThread;
    private Thread dataProcessingThread;

    // mqtt
    private MqttClient mqttClient;
    private static final String POLLUTION_TOPIC = "pollution/data";
    private static final String ENERGY_TOPIC = "energy/requests";
    private static final String MQTT_BROKER = "tcp://localhost:1883";

    // INIT
    public PowerPlant() {
    }

    public PowerPlant(String id, String listeningAddress, String adminServer) {
        this.plantId = id;
        this.listeningAddress = listeningAddress;
        this.adminServer = adminServer;

        // Buffer buffer = new Buffer();
        // this.sensor=new PollutionSensor(buffer);
    }

    public void initializePlant() {
        try {
            // TODO
            // introduceToExistingPlants(existingPlants);

            // Initialize MQTT client and subscribe to energy request topic
            initializeMqtt();

            // Start pollution data sensor and publish it on pollution data topic
            startSensor();

            startShutdownListener();
        } catch (Exception e) {
            // TODO
        }
    }

    // POLLUTION
    private void processAndSendPollutionData() {
        // List<Measurement> newMeasurements = sensor.getBuffer().readAllAndClean();
        slidingWindow.addAll(pollutionSensor.getBuffer().readAllAndClean());

        // Process sliding windows
        while (slidingWindow.size() >= 8) {
            // Take first 8 measurements
            List<Measurement> window = slidingWindow.subList(0, 8);

            // Compute average
            double average = window.stream()
                    .mapToDouble(Measurement::getValue)
                    .average()
                    .orElse(0.0);

            pendingAverages.add(average);

            // Slide window: remove first 4 (50% overlap)
            slidingWindow = new ArrayList<>(slidingWindow.subList(4, slidingWindow.size()));
        }

        // Send averages to server via MQTT
        if (!pendingAverages.isEmpty()) {
            publishPollutionData();
            pendingAverages.clear();
        }
    }

    public void startSensor() {
        SensorBuffer sensorBuffer = new SensorBuffer();
        pollutionSensor = new PollutionSensor(sensorBuffer);

        sensorThread = new Thread(pollutionSensor);
        sensorThread.start();

        startDataProcessing();
    }

    private void startDataProcessing() {
        dataProcessingThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) { // && !sensor.getStopCondition()) {
                try {
                    Thread.sleep(10000);
                    processAndSendPollutionData();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        dataProcessingThread.start();
    }

    // MQTT
    private void initializeMqtt() throws MqttException {
        String clientId = "plant_" + plantId;
        mqttClient = new MqttClient(MQTT_BROKER, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                if (ENERGY_TOPIC.equals(topic)){
                    String jsonMessage = new String(message.getPayload());
                    System.out.println("Received energy request: " + jsonMessage);
                    // TODO handle energy request (start election)
                }

            }

            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("MQTT connection lost!");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
            }
        });

        mqttClient.connect(options);
        mqttClient.subscribe(ENERGY_TOPIC);
    }

    private void publishPollutionData() {
        try {
            // Create JSON message
            JSONObject message = new JSONObject();
            message.put("plantId", plantId);
            message.put("timestamp", System.currentTimeMillis());

            JSONArray avgs = new JSONArray(pendingAverages);
            message.put("averages", avgs);

            // Publish to MQTT
            MqttMessage mqttMessage = new MqttMessage(message.toString().getBytes());
            mqttMessage.setQos(1); // At least once delivery

            mqttClient.publish(POLLUTION_TOPIC, mqttMessage);

            System.out.println("Sent pollution data: " + pendingAverages.size() + " averages");

        } catch (MqttException e) {
            System.err.println("Failed to send pollution data: " + e.getMessage());
            e.printStackTrace();
        }
    }



    private void disconnectMqttClients() {
        try {
            // Disconnect pollution MQTT client
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close();
            }

            // Disconnect energy request MQTT client
            if (mqttClient != null && mqttClient.isConnected()) { // energyMqttClient.disconnect();
            mqttClient.close();
            }

            System.out.println("MQTT clients disconnected.");

        } catch (Exception e) {
            System.err.println("Error disconnecting MQTT: " + e.getMessage());
        }
    }


// SHUTDOWN
    private void startShutdownListener() {
        Thread shutdownThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);

            while (!isShuttingDown) {
                String input = scanner.nextLine().trim().toLowerCase();

                if ("exit".equals(input)) {
                    System.out.println("Shutting down plant " + plantId + "...");
                    shutdown();
                    break;
                }
            }
        });

        shutdownThread.setDaemon(true); // Allows JVM to exit
        shutdownThread.start();
    }

    public void shutdown() {
        isShuttingDown = true;

        try {
            // 1. Stop sensor and processing
            stopSensorAndProcessing();

            // TODO
            // 2. Notify other plants (if needed for optional parts)
            // notifyOtherPlants();

            // 3. Notify administration server
            notifyAdminServerLeaving();

            // 4. Disconnect MQTT clients
            disconnectMqttClients();

            System.out.println("Plant " + plantId + " shutdown complete.");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
            System.exit(1);
        }
    }

    // ### MADE BY CLAUDE SONNET 4
    private void stopSensorAndProcessing() {
        try {
            // Stop the sensor simulator
            if (pollutionSensor != null) {
                pollutionSensor.stopMeGently();
            }

            // Interrupt and wait for threads to finish
            if (sensorThread != null && sensorThread.isAlive()) {
                sensorThread.interrupt();
                sensorThread.join(5000); // Wait up to 5 seconds
            }

            if (dataProcessingThread != null && dataProcessingThread.isAlive()) {
                dataProcessingThread.interrupt();
                dataProcessingThread.join(5000);
            }

            System.out.println("Sensor and processing threads stopped.");

        } catch (InterruptedException e) {
            System.err.println("Error stopping threads: " + e.getMessage());
        }
    }

    private void notifyAdminServerLeaving() {
            try {
                RestTemplate restTemplate = new RestTemplate();
            String url = "http://" + adminServer + "/plants/delete/" + plantId;
            
            restTemplate.delete(url);
            System.out.println("Successfully notified admin server of shutdown.");
            
        } catch (Exception e) {
            System.err.println("Error notifying admin server: " + e.getMessage());
        }
    }
    // ###


    

    // GETTERS and SETTERS
    public String getId() {
        return this.plantId;
    }

    public void setId(String id) {
        this.plantId = id;
    }

    public String getListeningAddress() {
        return this.listeningAddress;
    }

    public void setListeningAddress(String listeningAddress) {
        this.listeningAddress = listeningAddress;
    }

    public String getAdminServer() {
        return adminServer;
    }

    public void setAdminServer(String adminServer) {
        this.adminServer = adminServer;
    }

    @Override
    public String toString() {
        return "Id: " + plantId + ", listening on: " + listeningAddress;
    }

}