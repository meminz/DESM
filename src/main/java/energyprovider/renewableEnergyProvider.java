package energyprovider;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.*;
import org.json.JSONObject;

public class renewableEnergyProvider {
    
    private static final String MQTT_BROKER = "tcp://localhost:1883";
    private static final String ENERGY_TOPIC = "energy/requests/"; // ID will be added for each request
    private static final int REQUEST_INTERVAL = 10000; // milliseconds
    private static int ID = -1;
    private static Random rnd = new Random();

    private static MqttClient energyMqttClient;

    private static Thread publishingThread;
    private static volatile boolean isRunning = false;

    // MQTT
    public static void main(String[] args) {        
        try {
            initializeRequestMQTT();
        } catch (MqttException e) {
            System.out.println("Failed to connect to MQTT broker: " + e.getMessage());
            e.printStackTrace();
        }

        startPublishing();

    }

    private static void initializeRequestMQTT() throws MqttException {
        String clientId = "renewable_energy_provider";
        energyMqttClient = new MqttClient(MQTT_BROKER, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        energyMqttClient.connect(options);
        System.out.println("Connected to MQTT broker.");
    }

    private static void startPublishing(){
        isRunning = true;

        publishingThread = new Thread( () -> {
                System.out.println("Started publishing energy requests.");

                while (isRunning && !Thread.currentThread().isInterrupted()) {
                    try{
                        publishEnergyRequest();
                        Thread.sleep(REQUEST_INTERVAL);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    } catch (Exception e) {
                        System.out.println("Error publishing energy request: " + e.getMessage());
                    }
                }

                System.out.println("Stopped publishing energy requests.");
            }

        );

        publishingThread.run();
    }

    protected static void publishEnergyRequest() {
        try {
            int requestedEnergy = 5000; //+ rnd.nextInt(10001);

            // Create JSON message
            JSONObject message = new JSONObject();
            String thisId = "req-" + ++ID;
            message.put("requestId", thisId);
            message.put("timestamp", System.currentTimeMillis());
            message.put("energyAmount", requestedEnergy);

            // Publish to MQTT
            MqttMessage mqttMessage = new MqttMessage(message.toString().getBytes());
            mqttMessage.setQos(1); // QoS 2 would be more reliable but less efficient

            // energyMqttClient.publish(ENERGY_TOPIC, mqttMessage);
            // Topic with ID to handle retained messages and avoid overwriting a request
            energyMqttClient.publish(ENERGY_TOPIC + thisId, message.toString().getBytes(), 1, true);

            System.out.println("Published energy request of " + requestedEnergy + "kWh at " + ENERGY_TOPIC + thisId);

        } catch (MqttPersistenceException pe) {
            System.out.println("Failed to publish persistent request: " + pe.getMessage());
            pe.printStackTrace();

        } catch (MqttException e) {
            System.err.println("Failed to publish energy request: " + e.getMessage());
            e.printStackTrace();
        }

    }

}