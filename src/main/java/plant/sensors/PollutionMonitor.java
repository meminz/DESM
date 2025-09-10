package plant.sensors;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONArray;
import org.json.JSONObject;

import Simulators.Measurement;
import Simulators.PollutionSensor;

public class PollutionMonitor {
    private final String plantId;
    private final MqttClient mqttClient;
    private PollutionSensor pollutionSensor;
    private List<Measurement> slidingWindow = new ArrayList<>();
    private List<Double> pendingAverages = new ArrayList<>();
    private Thread dataProcessingThread;
    private final int SEND_INTERVAL;

    private static final String POLLUTION_TOPIC = "pollution/data";

    public PollutionMonitor(String plantId, MqttClient mqttClient, int interval) {
        this.plantId = plantId;
        this.mqttClient = mqttClient;
        this.SEND_INTERVAL=interval;
    }

    public void start() {
        SensorBuffer sensorBuffer = new SensorBuffer();
        pollutionSensor = new PollutionSensor(sensorBuffer);
        pollutionSensor.start();

        dataProcessingThread = new Thread(this::processingLoop);
        dataProcessingThread.start();
    }

    public void stop() {
        if (pollutionSensor != null)
            pollutionSensor.stopMeGently();
        if (dataProcessingThread != null)
            dataProcessingThread.interrupt();
    }

    private void processingLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(SEND_INTERVAL);
                processAndSendPollutionData();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void processAndSendPollutionData() {
        slidingWindow.addAll(pollutionSensor.getBuffer().readAllAndClean());

        while (slidingWindow.size() >= 8) {
            List<Measurement> window = slidingWindow.subList(0, 8);
            double average = window.stream().mapToDouble(Measurement::getValue).average().orElse(0.0);
            pendingAverages.add(average);
            slidingWindow = new ArrayList<>(slidingWindow.subList(4, slidingWindow.size()));
        }

        if (!pendingAverages.isEmpty())
            publishPollutionData();
    }

    private void publishPollutionData() {
        try {
            JSONObject message = new JSONObject();
            message.put("plantId", plantId);
            message.put("timestamp", System.currentTimeMillis());
            message.put("averages", new JSONArray(pendingAverages));

            MqttMessage mqttMessage = new MqttMessage(message.toString().getBytes());
            mqttMessage.setQos(1);
            mqttClient.publish(POLLUTION_TOPIC, mqttMessage);

            pendingAverages.clear();
        } catch (MqttException e) {
            System.err.println("Failed to send pollution data: " + e.getMessage());
        }
    }
}
