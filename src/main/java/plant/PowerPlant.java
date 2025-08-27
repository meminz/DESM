package plant;

import java.io.IOException;
import java.util.*;

import org.eclipse.paho.client.mqttv3.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.web.client.RestTemplate;

import Simulators.Measurement;
import Simulators.PollutionSensor;
import io.grpc.*;
import io.grpc.stub.StreamObserver;
import plant.grpc.PlantComms.*;
import plant.grpc.PlantCommunicationGrpc;
import plant.grpc.PlantCommunicationGrpc.PlantCommunicationImplBase;

public class PowerPlant {
    private String plantId;
    private String listeningAddress;
    private String adminServer;

    private volatile boolean isShuttingDown;
    private volatile boolean pendingShutdown = false;

    // ring network
    private String nextPlantAddress;
    private String prevPlantAddress;
    private String currentElectionId;
    private volatile boolean isProvidingEnergy = false;
    private volatile boolean isInElection = false;
    private volatile boolean isUpdatingRing = false;
    private final Object ringLock = new Object();
    private final Object electionLock = new Object();
    private final Random rnd = new Random();
    private Server grpcServer;
    private int grpcPort;
    private double myPrice;
    private final Map<String, String> allPlants = new HashMap<>(); // plantId -> address
    private List<String> sortedPlantIds = new ArrayList<>(); // maintains ring order

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

    /*
     * INIT
     */
    public PowerPlant() {}

    public PowerPlant(String id, String listeningAddress, String adminServer) {
        this.plantId = id;
        this.listeningAddress = listeningAddress;
        this.adminServer = adminServer;

        // Buffer buffer = new Buffer();
        // this.sensor=new PollutionSensor(buffer);
    }

    public void initializePlant(PlantInfo[] plants) {
        try {
            // Build local topology representation
            buildLocalTopology(plants);

            // Start gRPC server for plant2plant communication
            startGrpcServer();
            
            // Set up ring connections
            joinRingNetwork(plants);

            // Initialize MQTT client and subscribe to energy request topic
            initializeMqtt();

            // Start pollution data sensor and publish it on pollution data topic
            // startSensor();

            // Thread to handle stdin (only checks for exit command)
            startShutdownListener();

            System.out.println("Plant correctly initialized.\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /*
     * RING NETWORK
     */
    public void joinRingNetwork(PlantInfo[] existingPlants) {
        List<PlantInfo> tmpList = Arrays.asList(existingPlants);

        tmpList.sort(Comparator.comparing(PlantInfo::getId));

        int myIndex = findMyIndex(tmpList);
        int size = tmpList.size();
        
        // ### MADE BY CLAUDE SONNET 4
        if (size > 1) {
            int nextIndex = (myIndex + 1) % size;
            nextPlantAddress = tmpList.get(nextIndex).getListeningAddress();

            int prevIndex = (myIndex - 1 + size) % size;
            prevPlantAddress = tmpList.get(prevIndex).getListeningAddress();
        } else {
            nextPlantAddress = null;
            prevPlantAddress = null;
        }
        // ###

        System.out.println("Ring setup: " + prevPlantAddress + " --> " + plantId + " --> " + nextPlantAddress);

        sendGreetings();

    }

    
    private void sendGreetings() {
        if (nextPlantAddress == null) {
            System.out.println("No next plant - single node ring");
            return;
        }
        
        try {
            System.out.println("Sending initial greeting to next plant: " + nextPlantAddress);
            
            String[] parts = nextPlantAddress.split(":");
            ManagedChannel channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                .usePlaintext()
                .build();
            
            PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = 
                PlantCommunicationGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);
            
            GreetingsMessage greeting = GreetingsMessage.newBuilder()
                .setPlantId(this.plantId)
                .setListeningAddress(this.listeningAddress)
                .build();
            
            stub.sendGreetingsMessage(greeting);

        } catch (Exception e) {
            System.err.println("Error sending initial greeting: " + e.getMessage());
            e.printStackTrace();
        }
    }

        
    private int findMyIndex(List<PlantInfo> plants) {
        for (int i = 0; i < plants.size(); i++)
            if (plants.get(i).getId().equals(plantId))
                return i;

        return -1; // This can't happen
    }


    private void buildLocalTopology(PlantInfo[] plants) {
        synchronized (ringLock) {
            allPlants.clear();
            sortedPlantIds.clear();
            
            // Local topology
            for (PlantInfo plant : plants)
                allPlants.put(plant.getId(), plant.getListeningAddress());


            // Ring order
            for (String plantId : allPlants.keySet())
                sortedPlantIds.add(plantId);

            sortedPlantIds.sort(String::compareTo);
            
            System.out.println("Local topology built with " + allPlants.size() + " plants: " + sortedPlantIds);
        }
    }

    private void startElection(String requestId, int energyAmount) {
        synchronized(electionLock) {
            if (isProvidingEnergy) return;

            if (isInElection && requestId.equals(currentElectionId)) return;

            System.out.println("Starting election for request " + requestId);

            isInElection = true;
            currentElectionId = requestId;

            myPrice = 0.1 + (0.9 - 0.1) * rnd.nextDouble(); // in range [0.1; 0.9]
            System.out.println("Current price: " + myPrice);

            ElectionMessage msg = ElectionMessage.newBuilder()
                .setInitiatorId(plantId)
                .setCurrentHolderId(plantId)
                .setBestBid(myPrice)
                .setCurrentWinnerId(plantId)
                .setRequestId(requestId)
                .setEnergyRequest(energyAmount)
                .build();


            sendToNextPlant(msg);
        }
    }



    private void sendToNextPlant(ElectionMessage msg) {
        if (nextPlantAddress == null) {
            handleElectionWin(msg.getRequestId(), msg.getEnergyRequest());
            return;
        }

        System.out.println("Sending election message to the next plant at " + nextPlantAddress);


        ManagedChannel channel = null;

        try {
            // channel = ManagedChannelBuilder.forAddress(listeningAddress.split(":")[0], grpcPort)
            String[] parts = nextPlantAddress.split(":");
            channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
            .usePlaintext()
            .build();

            PlantCommunicationGrpc.PlantCommunicationBlockingStub stub =
                PlantCommunicationGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);

            System.out.println("Sending gRPC election message to: " + nextPlantAddress);
            
            ElectionResponse response = stub.sendElectionMessage(msg);

            if (response.getSuccess()) 
                System.out.println("Successfully sent election message");
            else
                System.out.println("Error sending election message");

        } catch (Exception e) {
            System.out.println("Error sending election message: " + e.getMessage());
            e.printStackTrace();

        } finally {
            if (channel != null) {
                try {
                    channel.shutdown().awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

    }

    public void handleElectionWin(String requestId, int energyAmount) {
        isProvidingEnergy = true;

        Thread provideEnergy = new Thread(() -> {
            try {
                System.out.println("\n============\nStarting energy production (" + energyAmount + "kWh)");

                Thread.sleep(energyAmount);

                System.out.println("Energy production completed for request " + requestId + "\n============\n");

            } catch (InterruptedException e) {
                System.out.println("Energy production interrupted: " + e.getMessage());
                Thread.currentThread().interrupt();

            } finally {
                isProvidingEnergy = false;
                isInElection = false;

                if (pendingShutdown)
                    plantShutdown();
            }
        });
        
        provideEnergy.start();

    }


    private void startGrpcServer() {
        try{
            grpcPort = Integer.parseInt(listeningAddress.split(":")[1]);

            grpcServer = ServerBuilder.forPort(grpcPort)
            .addService(new PlantCommsService())
            .build()
            .start();

            System.out.println("gRPC server started on port: " + grpcPort);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (grpcServer != null)
                    grpcServer.shutdown();
            }));

        } catch (IOException e) {
            System.out.println("Failed to start gRPC server: " + e.getMessage());
            e.printStackTrace();
        }
    }

// ### MADE BY CLAUDE 4
    private void handleNewPlantJoined(String newPlantId, String newListeningAddress) {
        synchronized (ringLock) {
            // If this greeting is from myself, the message completed the ring
            if (newPlantId.equals(this.plantId)) {
                System.out.println("Greeting message completed the ring. Topology update finished.");
                return;
            }

            System.out.println("Adding plant " + newPlantId + " to local topology");
            
            // Add new plant to local topology
            allPlants.put(newPlantId, newListeningAddress);
            
            // Rebuild sorted list
            sortedPlantIds.clear();
            for (String plantId : allPlants.keySet())
                sortedPlantIds.add(plantId);

            sortedPlantIds.sort(String::compareTo);

            // Recalculate my ring connections based on new topology
            recalculateRingConnections();

            
            System.out.println("Rewiring completed, forwarding message to " + nextPlantAddress);
            forwardGreetings(newPlantId, newListeningAddress);
       }
        
    }
// ###


    private void forwardGreetings(String originPlantId, String originAddress) {
        if (nextPlantAddress == null) {
            System.out.println("No next plant to forward to.");
            return;
        }
        
        int RETRIES = 3;
        int WAIT_TIME = 5000; // 5 sec

        System.out.println("Forwarding greeting to: " + nextPlantAddress + " (after rewiring completion)");

        for (int attempt = 0; attempt < RETRIES; attempt++) {
            try {
                String[] parts = nextPlantAddress.split(":");
                ManagedChannel channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                    .usePlaintext()
                    .build();
                
                PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = 
                    PlantCommunicationGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);
                
                GreetingsMessage greeting = GreetingsMessage.newBuilder()
                    .setPlantId(originPlantId)  // Keep original sender
                    .setListeningAddress(originAddress)
                    .build();
                
                GreetingsResponse response = stub.sendGreetingsMessage(greeting);
                System.out.println("Greeting forwarded successfully: " + response.getSuccess());
                
                channel.shutdown();
                break;
            } catch (Exception e) {
                System.out.println("Attempt " + attempt + " failed");
                System.err.println("Error forwarding greeting: " + e.getMessage());
                
                if (attempt < RETRIES - 1) {
                    System.out.println("Retrying forwarding greetings in " + WAIT_TIME + "ms...");

                    try {
                        Thread.sleep(WAIT_TIME);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                } else {
                    System.out.println("Failed to forward greetings message: " + e.getMessage());
                    e.printStackTrace();
                    recalculateRingConnections();
                }
                
            }
        }
        
    }

    private void forwardGreetingsAsync(String originPlantId, String originAddress) {
        if (nextPlantAddress == null) {
            System.out.println("No next plant to forward to.");
            return;
        }

        try {
            String[] parts = nextPlantAddress.split(":");
            ManagedChannel channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                .usePlaintext()
                .build();
            
            PlantCommunicationGrpc.PlantCommunicationStub asyncStub = 
                PlantCommunicationGrpc.newStub(channel)
                .withDeadlineAfter(10, java.util.concurrent.TimeUnit.SECONDS);
            
            GreetingsMessage greeting = GreetingsMessage.newBuilder()
                .setPlantId(originPlantId)  // Keep original sender
                .setListeningAddress(originAddress)
                .build();
            
            asyncStub.sendGreetingsMessage(
                greeting,
                new StreamObserver<GreetingsResponse>() {

                    @Override
                    public void onNext(GreetingsResponse response) {
                        System.out.println("Greetings message forwarded: " + response.getSuccess());
                    }

                    @Override
                    public void onError(Throwable t) {
                        System.out.println("Error forwarding greeting: "  + t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        System.out.println();
                        channel.shutdown();
                    };

                }
            );


        } catch (Exception e) {
            System.err.println("Error forwarding greeting: " + e.getMessage());
            
        }
        
    }


    private void recalculateRingConnections() {
        isUpdatingRing = true;

        int myIndex = sortedPlantIds.indexOf(plantId);
        int size = sortedPlantIds.size();
        
        if (size > 1) {
            // Calculate next plant
            int nextIndex = (myIndex + 1) % size;
            String nextId = sortedPlantIds.get(nextIndex);
            nextPlantAddress = allPlants.get(nextId);
            
            // Calculate previous plant
            int prevIndex = (myIndex - 1 + size) % size;
            String prevId = sortedPlantIds.get(prevIndex);
            prevPlantAddress = allPlants.get(prevId);
        } else {
            nextPlantAddress = null;
            prevPlantAddress = null;
        }

        isUpdatingRing = false;

        System.out.println("Updated topology: " + sortedPlantIds);
        System.out.println("My connections: " + prevPlantAddress + " --> " + plantId + " --> " + nextPlantAddress);

    }
// ###


    private void handleIncomingElectionMessage(ElectionMessage proto) {

        synchronized (electionLock) {
            String initiatorId = proto.getInitiatorId();
            String currentHolderId = proto.getCurrentHolderId();
            double bestBid = proto.getBestBid();
            String currentWinnerId = proto.getCurrentWinnerId();
            String requestId = proto.getRequestId();
            int energyAmount = proto.getEnergyRequest();

            System.out.println(requestId + " |==> Processing election message: initiator=" + initiatorId + 
                          ", bestBid=" + bestBid + 
                          ", bestCandidate=" + currentWinnerId);

            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                // TODO: handle exception
            }

            
            // This should never happen
            if (initiatorId.equals(plantId) && currentHolderId.equals(plantId)) {
                System.out.println("WTF - Received message from myself");
                return;   
            }


            // If my message comes back to me, election is complete
            if (initiatorId.equals(plantId)) {

                System.out.println("Election message returned to initiator - election complete!\n");
                handleElectionComplete(currentWinnerId, requestId, energyAmount);

                return;
            }


            ElectionMessage updatedMessage = proto.toBuilder()
                .setCurrentHolderId(plantId)
                .build();


            // If I'm busy, just forward the message
            if (isProvidingEnergy) {
                System.out.println("Busy..., forwarding election message");
                sendToNextPlant(updatedMessage);
                return;
            }

            // If I joined mid election, just forward the original message
            if (!isInElection && currentElectionId == null) {
                System.out.println("Joined during election..., forwarding election message");
                sendToNextPlant(proto);
                return;
            }


            if (ImBetterCandidate(myPrice, plantId, bestBid, currentWinnerId)) {
                System.out.println("I'm a better candidate: " + myPrice + " < " + bestBid + "\tNot forwarding.");
                updatedMessage = updatedMessage.toBuilder()
                    .setBestBid(myPrice)
                    .setCurrentWinnerId(plantId)
                    .build();

                System.out.println("Plant " + plantId + " is now the best candidate");

            } else
                System.out.println("I'm not the best candidate: " + myPrice + " > " + bestBid);

            sendToNextPlant(updatedMessage);
        }
    }

    
    private void handleElectionComplete(String winnerId, String requestId, int energyAmount) {
        synchronized (electionLock) {
            System.out.println("\n--- Election Complete ---" +
            "\nWinner: " + winnerId + " for request " + requestId);

            if (winnerId.equals(plantId))
                handleElectionWin(requestId, energyAmount);

            isInElection = false;
            currentElectionId = null;

            System.out.println("Election finished - ready for next request\n");

            if (pendingShutdown)
                plantShutdown();
        }
        
    }

    private boolean ImBetterCandidate(double myPrice, String plantId, double bestBid, String currentWinnerId) {
        if (myPrice == bestBid)
            return plantId.compareTo(currentWinnerId) > 0;

        return myPrice < bestBid;
    }

    private void handlePlantLeaving(FarewellMessage farewell) {
        String leavingPlantId = farewell.getPlantId();
        String prevPlantId = farewell.getPrevPlantId();


        allPlants.remove(leavingPlantId);
        sortedPlantIds.removeIf(i -> i.equals(leavingPlantId));

        recalculateRingConnections();

        if (prevPlantId.equals(plantId)) {
            System.out.println("All plants notified of plant " + leavingPlantId + " shutdown.");
            return;
        }

        String[] parts = nextPlantAddress.split(":");

        ManagedChannel channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
            .usePlaintext()
            .build();
        
        PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = 
            PlantCommunicationGrpc.newBlockingStub(channel)
            .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);

        FarewellResponse response = stub.sendFarewellMessage(farewell);

        if (response.getSuccess())
            System.out.println("Farewell message successfully forwarded.");

    }
    
    /*
     * gRPC
     */
    private class PlantCommsService extends PlantCommunicationImplBase {
    // Service as a subclass to handle synchronization problems easier

        @Override
        public void sendGreetingsMessage(GreetingsMessage request, StreamObserver<GreetingsResponse> responseObserver) {
            try {
                System.out.println("New plant introduced: " + request.getPlantId() + "@" + request.getListeningAddress());

                GreetingsResponse response = GreetingsResponse.newBuilder()
                    .setSuccess(true)
                    // .setIsInElection(isInElection)
                    // .setMessage("Plant introduction processed")
                    .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                // Run in ROOT context, otherwise deadline will be passed on
                Context.ROOT.run(() -> {
                    handleNewPlantJoined(request.getPlantId(), request.getListeningAddress());
                });
                
            } catch (Exception e) {
                System.out.println("Error processing plant introduction: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void sendElectionMessage(ElectionMessage request, StreamObserver<ElectionResponse> responseObserver) {
            try{
                System.out.println("Received election message from: " + request.getCurrentHolderId());


                ElectionResponse response = ElectionResponse.newBuilder()
                    .setSuccess(true)
                    // .setMessage("Election message processed")
                    .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();


                Context.ROOT.run(() -> {
                    handleIncomingElectionMessage(request);
                });

            } catch (Exception e) {
                System.out.println("Error processing election message: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // No winner message for simplicity (we don't need to coordinate any work)

        @Override
        public void sendFarewellMessage(FarewellMessage request, StreamObserver<FarewellResponse> responseObserver) {
            try {
                System.out.println("Received farewell from plant " + request.getPlantId());

                FarewellResponse response = FarewellResponse.newBuilder()
                    .setSuccess(true)
                    .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                Context.ROOT.run(() -> {
                    handlePlantLeaving(request);
                });

            } catch (Exception e) {
                System.out.println("Error processing farewell message: " + e.getMessage());
                e.printStackTrace();
            }

        }
    }

    /*
     * POLLUTION (sensing)
     */
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
        if (!pendingAverages.isEmpty())
            publishPollutionData();
    }

    public void startSensor() {
        SensorBuffer sensorBuffer = new SensorBuffer();
        pollutionSensor = new PollutionSensor(sensorBuffer);

        sensorThread = new Thread(pollutionSensor);
        sensorThread.start();
        System.out.println("Started pollution sensor...");

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
    // ###


    /*
     * MQTT
     */
    private void initializeMqtt() throws MqttException {
        String clientId = "plant_" + plantId;
        mqttClient = new MqttClient(MQTT_BROKER, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);

        mqttClient.setCallback(new MqttCallback() {
            @Override
            public void messageArrived(String topic, MqttMessage message) {
            try{
                if (ENERGY_TOPIC.equals(topic)){
                    String jsonMessage = new String(message.getPayload());
                    JSONObject json = new JSONObject(jsonMessage);
                    String requestId = json.getString("requestId");
                    int energyAmount = json.getInt("energyAmount");

                    System.out.println("Received energy request: " + energyAmount + "kWh (ID: " + requestId + ")");

                    
                    // Handle energy request (start election)
                    startElection(requestId, energyAmount);
                }
            } catch (Exception e) {
                e.printStackTrace();
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

    /*
     * SHUTDOWN
     */
    private void startShutdownListener() {
    // Actually a stdin listener, but for now there is just the exit command
        Thread shutdownThread = new Thread(() -> {
            Scanner scanner = new Scanner(System.in);

            while (!isShuttingDown) {
                String input = scanner.nextLine().trim().toLowerCase();

                if ("exit".equals(input)) {
                    System.out.println("Shutting down plant " + plantId + "...");
                    plantShutdown();
                    break;
                }
            }
        });

        shutdownThread.setDaemon(true); // Allows JVM to exit
        shutdownThread.start();
    }

    public void plantShutdown() {
        isShuttingDown = true;

        if (isInElection || isProvidingEnergy) {
            System.out.println(
                isInElection ? "Currently in election, shutting down later..." : "Currently providing energy, shutting down late"
            );
            pendingShutdown = true;
            return;
        }

        try {
            // 1. Notify other plants 
            if (nextPlantAddress != null)
                notifyOtherPlants();

            // 2. Notify administration server
            notifyAdminServerLeaving();

            // 3. Disconnect MQTT clients
            disconnectMqttClients();
            
            // 4. Stop sensor and processing
            stopSensorAndProcessing();

            System.out.println("Plant " + plantId + " shutdown complete.");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("Error during shutdown: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void notifyOtherPlants() {
        try {
            String[] parts = nextPlantAddress.split(":");

            ManagedChannel channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                .usePlaintext()
                .build();
            
            PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = 
                PlantCommunicationGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);
            
            int prevPlantIndex = sortedPlantIds.indexOf(plantId) - 1;
            if (prevPlantIndex < 0)
                prevPlantIndex += sortedPlantIds.size();

            FarewellMessage farewell = FarewellMessage.newBuilder()
                .setPlantId(this.plantId)
                .setPrevPlantId(sortedPlantIds.get(prevPlantIndex))
                .build();
            
            FarewellResponse response = stub.sendFarewellMessage(farewell);

            if (response.getSuccess()) {
                System.out.println("Farewell message sent.");
            }

        } catch (Exception e) {
            System.err.println("Error notifying other plants: " + e.getMessage());
            e.printStackTrace();
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


    
    /*
     * GETTERS and SETTERS
     */
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