package plant;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

import plant.models.EnergyRequestInfo;
import plant.models.PlantInfo;

public class RingNetwork {
    private final String plantId;
    private final String listeningAddress;
    private final Map<String, String> allPlants = new HashMap<>();
    private List<String> sortedPlantIds = new ArrayList<>();
    private String nextPlantAddress;
    private String prevPlantAddress;
    
    // Election state
    private volatile boolean isInElection = false;
    private volatile boolean isProvidingEnergy = false;
    private String currentElectionId;
    private double myPrice;
    
    // Request handling
    private final Queue<EnergyRequestInfo> pendingRequests = new LinkedList<>();
    private final Set<String> seenRequests = new HashSet<>();
    private final Set<String> completedRequests = new HashSet<>();
    private final Map<String, Long> electionStartTimes = new HashMap<>();
    
    private final Object ringLock = new Object();
    private final Object electionLock = new Object();
    private final Object requestQueueLock = new Object();
    private final Random rnd = new Random();
    
    private static final long ELECTION_TIMEOUT = 30000;
    private GrpcService grpcService; // Will be injected
    private final EnergyProductionCallback energyProductionCallback;

    // Shutdown
    private volatile boolean isShuttingDown = false;
    private final Object shutdownLock = new Object();
    private volatile boolean shutdownReady = false;

    public interface EnergyProductionCallback {
        void onEnergyProduction(String requestId, int energyAmount);
    }

    public RingNetwork(String plantId, String listeningAddress, EnergyProductionCallback energyProductionCallback) {
        this.plantId = plantId;
        this.listeningAddress=listeningAddress;
        this.energyProductionCallback = energyProductionCallback;
        startTimeoutChecker();
    }

    public void setGrpcService(GrpcService grpcService) {
        this.grpcService = grpcService;
    }

    protected void buildRingTopology(PlantInfo[] plants) {
        synchronized (ringLock) {
            allPlants.clear();
            sortedPlantIds.clear();

            for (PlantInfo plant : plants) {
                allPlants.put(plant.getId(), plant.getListeningAddress());
            }

            for (String plantId : allPlants.keySet()) {
                sortedPlantIds.add(plantId);
            }

            sortedPlantIds.sort(String::compareTo);
            System.out.println("Local topology built with " + allPlants.size() + " plants: " + sortedPlantIds);
        }
    }

    protected void joinRingNetwork(PlantInfo[] existingPlants, GrpcService grpcService) {
        this.grpcService = grpcService;
        
        List<PlantInfo> tmpList = Arrays.asList(existingPlants);
        tmpList.sort(Comparator.comparing(PlantInfo::getId));

        int myIndex = tmpList.indexOf(plantId);
        int size = tmpList.size();

        if (size > 1) {
            int nextIndex = (myIndex + 1) % size;
            nextPlantAddress = tmpList.get(nextIndex).getListeningAddress();

            int prevIndex = (myIndex - 1 + size) % size;
            prevPlantAddress = tmpList.get(prevIndex).getListeningAddress();
        } else {
            nextPlantAddress = null;
            prevPlantAddress = null;
        }

        System.out.println("Ring setup: " + prevPlantAddress + " --> " + plantId + " --> " + nextPlantAddress);
        if (nextPlantAddress != null)
            grpcService.forwardGreetingMessage(plantId, listeningAddress, nextPlantAddress);
    }


    protected void handleEnergyRequest(String requestId, int energyAmount) {
        synchronized (requestQueueLock) {
            if (seenRequests.contains(requestId) || completedRequests.contains(requestId)) {
                System.out.println("Ignoring duplicate/completed request: " + requestId);
                return;
            }

            seenRequests.add(requestId);
            EnergyRequestInfo request = new EnergyRequestInfo(requestId, energyAmount);
            pendingRequests.offer(request);
            System.out.println("Queued request " + requestId + " (queue size: " + pendingRequests.size() + ")");
        }
        
        synchronized (electionLock) {
            tryProcessNextRequest();
        }
    }
    
    private void tryProcessNextRequest() {
        // Can only start new election if completely idle
        if (isShuttingDown || isProvidingEnergy || isInElection || pendingRequests.isEmpty())
            return;

        EnergyRequestInfo request = null;
        synchronized (requestQueueLock) {
            request = pendingRequests.poll();
        }

        if (request != null) {
            System.out.println("Starting election for queued request: " + request.getId());
            startElection(request.getId(), request.getEnergyAmount());
        }
    }

    private void startElection(String requestId, int energyAmount) {
        isInElection = true;
        currentElectionId = requestId;
        electionStartTimes.put(requestId, System.currentTimeMillis());
        myPrice = 0.1 + (0.9 - 0.1) * rnd.nextDouble();

        System.out.println("Starting election for request " + requestId + ", current price: " + myPrice);

        if (nextPlantAddress == null) {
            synchronized (electionLock) {
                handleElectionComplete(requestId, plantId, energyAmount);
            }
            return;
        }

        grpcService.forwardElectionMessage(requestId, energyAmount,
            plantId, plantId, myPrice, nextPlantAddress);
    }

    // protected void handleIncomingElectionMessage(ElectionMessage proto) {
    protected void handleIncomingElectionMessage(String requestId, int energyAmount, String initiatorId, String currentWinnerId, double bestBid) {
        synchronized (requestQueueLock) {
            if (!seenRequests.contains(requestId)) {// Joining an active election
                seenRequests.add(requestId);

                if (!isInElection && !isProvidingEnergy)
                    pendingRequests.offer(new EnergyRequestInfo(requestId, energyAmount));
            }
        }
        
        synchronized (electionLock) {
            System.out.println(requestId + " |==> Processing election message: initiator=" + initiatorId +
                    ", bestBid=" + bestBid +
                    ", bestCandidate=" + currentWinnerId);

            try {
                Thread.sleep(5000);
            } catch (Exception e) {
                e.printStackTrace();
            }

            // If my message comes back to me, election is complete
            if (initiatorId.equals(plantId)) {
                System.out.println("Election message returned to initiator - election complete!\n");
                handleElectionComplete(currentWinnerId, requestId, energyAmount);
                return;
            }

            // If I'm busy, just forward the message
            // if (isProvidingEnergy) {
            //     System.out.println("Busy providing energy..., forwarding election message");
            //     grpcService.forwardElectionMessage(updatedMessage, nextPlantAddress);
            //     return;
            // }

            // Actual election participation
            if (isBetterCandidate(myPrice, plantId, bestBid, currentWinnerId)) {
                System.out.println("I'm a better candidate: " + myPrice + " < " + bestBid);
                grpcService.forwardElectionMessage(requestId, energyAmount, initiatorId,
                    plantId, myPrice, nextPlantAddress);

            } else
                System.out.println("I'm not the best candidate: " + myPrice + " > " + bestBid);
                grpcService.forwardElectionMessage(requestId, energyAmount, initiatorId,
                    currentWinnerId, bestBid, nextPlantAddress);
        }
    }

    private boolean isBetterCandidate(double myPrice, String plantId, double bestBid, String currentWinnerId) {
        if (myPrice == bestBid)
            return plantId.compareTo(currentWinnerId) > 0;
        
        return myPrice < bestBid;
    }


    // UNSAFE, must be called with election lock
    protected void handleElectionComplete(String requestId, String winnerId, int energyAmount) {
        System.out.println("\n--- Election Complete ---" +
                "\nWinner: " + winnerId + " for request " + requestId);

        synchronized (requestQueueLock) {
            completedRequests.add(requestId);
            pendingRequests.removeIf(r -> r.getId().equals(requestId));
        }

        electionStartTimes.remove(requestId);

        if (winnerId.equals(plantId))
            handleElectionWin(requestId, energyAmount);
        else {
            resetElectionState();
            
            if (isShuttingDown && !isProvidingEnergy)
                signalShutdownReady();
            else if (!isShuttingDown)
                tryProcessNextRequest();

        }

        System.out.println("Election finished - ready for next request\n");
    }

    // UNSAFE, must be called with election lock
    public void handleElectionWin(String requestId, int energyAmount) {
        isProvidingEnergy = true;

        energyProductionCallback.onEnergyProduction(requestId, energyAmount);
        resetElectionState();

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
                
                if (isShuttingDown)
                    signalShutdownReady();
                else
                    tryProcessNextRequest();
            }

        });

        provideEnergy.start();
    }

    private void resetElectionState() {
        isInElection = false;
        currentElectionId = null;
        myPrice = 100;
    }

    // ### MADE BY CLAUDE 4
    protected void handleNewPlantJoined(String newPlantId, String newListeningAddress) {
        // If this greeting is from myself, the message completed the ring
        if (newPlantId.equals(this.plantId)) {
            System.out.println("Greeting message completed the ring. Topology update finished.\n");
            return;
        }

        System.out.println("Adding plant " + newPlantId + " to local topology");

        synchronized (ringLock) {
            // Add new plant to local topology
            allPlants.put(newPlantId, newListeningAddress);

            // Rebuild sorted list
            sortedPlantIds.clear();
            for (String plantId : allPlants.keySet())
                sortedPlantIds.add(plantId);

            sortedPlantIds.sort(String::compareTo);
        }

        // Recalculate my ring connections based on new topology
        recalculateRingConnections();

        System.out.println("Rewiring completed, forwarding message to " + nextPlantAddress + "\n");
        grpcService.forwardGreetingMessage(newPlantId, newListeningAddress, nextPlantAddress);

    }
    // ###

    
    // protected void handlePlantLeaving(FarewellMessage farewell) {
    protected void handlePlantLeaving(String leavingPlantId, String prevPlantId) {
        synchronized (ringLock) {
            allPlants.remove(leavingPlantId);
            sortedPlantIds.removeIf(i -> i.equals(leavingPlantId));
        }

        recalculateRingConnections();

        if (prevPlantId.equals(plantId)) {
            System.out.println("All plants notified of plant " + leavingPlantId + " shutdown.\n");
            return;
        }

        grpcService.forwardFarewellMessage(leavingPlantId, prevPlantId, nextPlantAddress);
    }


    protected void leaveRingNetwork() {
        if (isInElection || isProvidingEnergy) {
            System.out.println("Busy, shutting down later");
            return;
        }

        if (nextPlantAddress != null) {
            int prevPlantIndex = sortedPlantIds.indexOf(plantId) - 1;
            if (prevPlantIndex < 0)
                prevPlantIndex += sortedPlantIds.size();

            System.out.println("Notifying other plants of shutdown...");
            grpcService.forwardFarewellMessage(plantId, sortedPlantIds.get(prevPlantIndex), nextPlantAddress);
        }
    }


    protected void recalculateRingConnections() {
        synchronized (ringLock) {
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
        }

        System.out.println("Updated topology: " + sortedPlantIds);
        System.out.println("My connections: " + prevPlantAddress + " --> " + plantId + " --> " + nextPlantAddress);

    }

    // TIMEOUT
    private void startTimeoutChecker() {
        Thread timeoutThread = new Thread(() -> {
            while (!isShuttingDown) {
                try {
                    Thread.sleep(5000); // Check every 5 seconds
                    checkForTimeouts();
                } catch (InterruptedException e) {
                    break;
                }
            }
        });
        timeoutThread.setDaemon(true);
        timeoutThread.start();
    }

    private void checkForTimeouts() {
        synchronized (electionLock) {
            if (currentElectionId != null) {
                Long startTime = electionStartTimes.get(currentElectionId);

                if (startTime != null && System.currentTimeMillis() - startTime > ELECTION_TIMEOUT) {
                    System.out.println("Election timeout for " + currentElectionId + ", resetting...");

                    // Reset election state
                    String timedOutElection = currentElectionId;
                    resetElectionState();
                    electionStartTimes.remove(timedOutElection);

                    // Try to process next request
                    if (isShuttingDown && !isProvidingEnergy)
                        signalShutdownReady();
                    else if (!isShuttingDown)
                        tryProcessNextRequest();
                }
            }
        }
    }

    public void initiateShutdown() {
        synchronized (electionLock) {
            isShuttingDown = true;
            // pendingShutdown = true;
            
            // If we're currently idle, we can shutdown immediately
            if (!isInElection && !isProvidingEnergy) {
                signalShutdownReady();
            }
        }
    }

    public boolean waitForShutdownComplete(long timeoutMs) {
        synchronized (shutdownLock) {
            if (shutdownReady)
                return true; // Already ready
            
            long startTime = System.currentTimeMillis();
            long remainingTime = timeoutMs;
            
            while (!shutdownReady && remainingTime > 0) {
                try {
                    shutdownLock.wait(remainingTime/2);
                    remainingTime = timeoutMs - (System.currentTimeMillis() - startTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
            
            return shutdownReady;
        }
    }

    private void signalShutdownReady() {
        synchronized (shutdownLock) {
            if (!shutdownReady) {
                shutdownReady = true;
                shutdownLock.notifyAll();
            }
        }
    }

/*
 * GETTERS and SETTERS
 */
    public String getPlantId() {
        return plantId;
    }

    public String getListeningAddress() {
        return listeningAddress;
    }

    public Map<String, String> getAllPlants() {
        return allPlants;
    }

    public boolean hasOtherPlants() {
        return allPlants.size() > 1;
    }

    public List<String> getSortedPlantIds() {
        return sortedPlantIds;
    }

    public void setSortedPlantIds(List<String> sortedPlantIds) {
        this.sortedPlantIds = sortedPlantIds;
    }

    public String getNextPlantAddress() {
        return nextPlantAddress;
    }

    public String getPrevPlantAddress() {
        return prevPlantAddress;
    }

    public boolean isProvidingEnergy() {
        return isProvidingEnergy;
    }

    public boolean canShutdown() {
        synchronized (electionLock) {
            return !isInElection && !isProvidingEnergy;
        }
    }

}
