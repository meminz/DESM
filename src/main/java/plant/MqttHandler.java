package plant;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONObject;

public class MqttHandler {
    
    private final String plantId;
    private MqttClient mqttClient;
    private final EnergyRequestCallback energyRequestCallback;
    
    private static final String ENERGY_TOPIC = "energy/requests/";
    private static final String MQTT_BROKER = "tcp://localhost:1883";

    public interface EnergyRequestCallback {
        void onEnergyRequest(String requestId, int energyAmount);
    }

    public MqttHandler(String plantId, EnergyRequestCallback callback) {
        this.plantId = plantId;
        this.energyRequestCallback = callback;
    }

    public void initialize() throws MqttException {
        String clientId = "plant_" + plantId;
        mqttClient = new MqttClient(MQTT_BROKER, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(false);

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
                if (!topic.startsWith(ENERGY_TOPIC) || message.getPayload().length == 0) return;

                try {
                    String jsonMessage = new String(message.getPayload());
                    JSONObject json = new JSONObject(jsonMessage);
                    String requestId = json.getString("requestId");
                    int energyAmount = json.getInt("energyAmount");
                    
                    energyRequestCallback.onEnergyRequest(requestId, energyAmount);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void connectionLost(Throwable cause) {
                System.err.println("MQTT connection lost!");
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {}
        });

        mqttClient.connect(options);
        mqttClient.subscribe(ENERGY_TOPIC + "+", 1);
    }

    public void removeRetainedRequest(String requestId) {
        System.out.println("Removing retained request " + requestId);
        try {
            mqttClient.publish(ENERGY_TOPIC + requestId, new byte[0], 1, true);
        } catch (Exception e) {
            System.err.println("Failed to remove retained request: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public MqttClient getClient() {
        return mqttClient;
    }

    public boolean disconnect() {
        try {
            if (mqttClient != null && mqttClient.isConnected()) {
                mqttClient.disconnect();
                mqttClient.close();
            }
            
            return true;
        } catch (Exception e) {
            System.err.println("Error disconnecting MQTT: " + e.getMessage());

            return false;
        }
    }
}