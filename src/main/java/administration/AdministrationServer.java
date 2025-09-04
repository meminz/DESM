package administration;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import administration.model.PollutionReading;

@SpringBootApplication
public class AdministrationServer {
    public static void main(String[] args) {
        // Start Spring Boot
        SpringApplication.run(AdministrationServer.class, args);
        System.out.println("Server running on http://localhost:8080");

        // Start MQTT subscriber for pollution data
        startMqttSubscriber();
    }

    // in-memory storage for pollution data
    private static TreeMap<Long, List<PollutionReading>> pollutionByTimestamp = new TreeMap<>();


//### MADE BY CLAUDE SONNET 4
    public static void storePollutionData(String jsonMessage) {
        try {
            JSONObject json = new JSONObject(jsonMessage);
            String plantId = json.getString("plantId");
            long timestamp = json.getLong("timestamp");
            
            List<Double> averages = new ArrayList<>();
            JSONArray avgArray = json.getJSONArray("averages");
            for (int i = 0; i < avgArray.length(); i++) {
                averages.add(avgArray.getDouble(i));
            }
            
            PollutionReading reading = new PollutionReading(plantId, timestamp, averages);
            
            // Store by timestamp - multiple readings can have same timestamp
            synchronized(pollutionByTimestamp) {
                pollutionByTimestamp.computeIfAbsent(timestamp, k -> new ArrayList<>())
                .add(reading);
            }
            
        } catch (Exception e) {
            System.err.println("Error storing pollution data: " + e.getMessage());
        }
    }


    public static double calculateAverageBetweenTimestamps(long t1, long t2) {
        if (pollutionByTimestamp.isEmpty() || pollutionByTimestamp == null) {
            System.out.println("No pollution data available.");
            return 0.0;
        }

        List<Double> matchingValues = new ArrayList<>();

        synchronized(pollutionByTimestamp) {
            // Get all entries between t1 and t2
            NavigableMap<Long, List<PollutionReading>> range = 
                pollutionByTimestamp.subMap(t1, true, t2, true);

            for (List<PollutionReading> readings : range.values()) {
                for (PollutionReading reading : readings) {
                    matchingValues.addAll(reading.getAverages());
                }
            }
        }

        return matchingValues.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(0.0);
    }
//###




// MQTT
    // subscriber for pollution data
    private static void startMqttSubscriber() {
        try {
            @SuppressWarnings("resource")
            MqttClient client = new MqttClient("tcp://localhost:1883", "admin_server");

            client.setCallback(new MqttCallback() {
                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    String jsonMessage = new String(message.getPayload());
                    System.out.println("Received pollution data: " + jsonMessage);

                    storePollutionData(jsonMessage);
                }

                @Override
                public void connectionLost(Throwable cause) {
                    System.err.println("MQTT connection lost!");
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                }
            });

            client.connect();
            client.subscribe("pollution/data");
            System.out.println("MQTT subscriber started - listening for pollution data");

        } catch (MqttException e) {
            System.err.println("Failed to start MQTT: " + e.getMessage());
        }
    }

}