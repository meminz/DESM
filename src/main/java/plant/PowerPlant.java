package plant;


public class PowerPlant {
    private String plantId;
    private String listeningAddress;
    private String adminServerAddress;
    
    // Components
    private RingNetwork ringNetwork;
    private GrpcService grpcService;
    private MqttHandler mqttHandler;
    private PollutionMonitor pollutionMonitor;
    private PlantClient plantClient;
    
    public PowerPlant() {
    }

    public PowerPlant(String id, String listeningAddress, String adminServerAddress) {
        this.plantId = id;
        this.listeningAddress = listeningAddress;
        this.adminServerAddress = adminServerAddress;
        
        // Initialize components
        this.plantClient = new PlantClient(adminServerAddress);
        this.ringNetwork = new RingNetwork(plantId, listeningAddress);
        this.mqttHandler = new MqttHandler(plantId, this::handleEnergyRequest);
        this.grpcService = new GrpcService(listeningAddress, ringNetwork);
    }

    public void initializePlant(PlantInfo[] plants) {
        try {
            // Initialize MQTT first (needed for pollution monitor)
            mqttHandler.initialize();
            
            // Initialize pollution monitoring
            pollutionMonitor = new PollutionMonitor(plantId, mqttHandler.getClient());
            pollutionMonitor.start();
            
            // Initialize ring network and gRPC
            ringNetwork.buildRingTopology(plants);
            grpcService.start();
            ringNetwork.joinRingNetwork(plants, grpcService);
            
            // startStdinListener();
            
            System.out.println("Plant correctly initialized.\n");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleEnergyRequest(String requestId, int energyAmount) {
        // Delegate to ring network
        ringNetwork.handleEnergyRequest(requestId, energyAmount);
    }

    public void shutdown() {
        try {
            ringNetwork.setPendingShutdown(true);

            // Shutdown sequence
            if (ringNetwork.hasOtherPlants()) {
                System.out.println("Notifying other plants of shutdown...");
                ringNetwork.leaveRingNetwork();
            }
            
            plantClient.notifyLeaving(plantId);
            pollutionMonitor.stop();
            mqttHandler.disconnect();
            grpcService.stop();

            System.out.println("Plant " + plantId + " shutdown complete.");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    // Getters - needed for JSON serialization by PlantClient
    public String getId() { return plantId; }
    public String getListeningAddress() { return listeningAddress; }
    public String getAdminServer() { return adminServerAddress; }
    
    // Setters - needed for JSON deserialization (if used)
    public void setId(String id) { /* ignore - final field */ }
    public void setListeningAddress(String address) { /* ignore - final field */ }
    public void setAdminServer(String server) { /* ignore - final field */ }
    
    @Override
    public String toString() {
        return "Id: " + plantId + ", listening on: " + listeningAddress;
    }
}