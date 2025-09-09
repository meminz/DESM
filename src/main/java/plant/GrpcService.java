package plant;

import java.io.IOException;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import plant.grpc.PlantComms.ElectionMessage;
import plant.grpc.PlantComms.ElectionResponse;
import plant.grpc.PlantComms.FarewellMessage;
import plant.grpc.PlantComms.FarewellResponse;
import plant.grpc.PlantComms.GreetingsMessage;
import plant.grpc.PlantComms.GreetingsResponse;
import plant.grpc.PlantCommunicationGrpc;
import plant.grpc.PlantCommunicationGrpc.PlantCommunicationImplBase;

public class GrpcService {
    private final int grpcPort;
    private Server grpcServer;
    private final RingNetwork ringNetwork;

    public GrpcService(String listeningAddress, RingNetwork ringNetwork) {
        this.grpcPort = Integer.parseInt(listeningAddress.split(":")[1]);
        this.ringNetwork = ringNetwork;
    }

    public void start() {
        try {
            grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(new PlantCommsService())
                    .build()
                    .start();
        } catch (IOException e) {
            System.out.println("Failed to start gRPC server: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("gRPC server started on port: " + grpcPort);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (grpcServer != null)
                grpcServer.shutdown();
        }));
    }

    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
    }


    private class PlantCommsService extends PlantCommunicationImplBase {

        @Override
        public void sendGreetingsMessage(GreetingsMessage request, StreamObserver<GreetingsResponse> responseObserver) {
            try {
                System.out
                        .println("New plant introduced: " + request.getPlantId() + "@" + request.getListeningAddress());

                GreetingsResponse response = GreetingsResponse.newBuilder()
                        .setSuccess(true)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                // Handle in separate context to avoid deadline propagation
                Context.ROOT.run(() -> {
                    ringNetwork.handleNewPlantJoined(request.getPlantId(), request.getListeningAddress());
                });

            } catch (Exception e) {
                System.out.println("Error processing plant introduction: " + e.getMessage());
                e.printStackTrace();
            }
        }

        @Override
        public void sendElectionMessage(ElectionMessage request, StreamObserver<ElectionResponse> responseObserver) {
            try {
                System.out.println("Received election message from: " + request.getCurrentHolderId());

                ElectionResponse response = ElectionResponse.newBuilder()
                        .setSuccess(true)
                        .build();

                responseObserver.onNext(response);
                responseObserver.onCompleted();

                // ? TODO unmarshall directly into function call
                String requestId = request.getRequestId();
                int energyAmount = request.getEnergyRequest();
                String initiatorId = request.getInitiatorId();
                String currentWinnerId = request.getCurrentWinnerId();
                double bestBid = request.getBestBid();

                Context.ROOT.run(() -> {
                    ringNetwork.handleIncomingElectionMessage(requestId, energyAmount, initiatorId, currentWinnerId, bestBid);
                });

            } catch (Exception e) {
                System.out.println("Error processing election message: " + e.getMessage());
                e.printStackTrace();
            }
        }

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
                    ringNetwork.handlePlantLeaving(request.getPlantId(), request.getPrevPlantId());
                });

            } catch (Exception e) {
                System.out.println("Error processing farewell message: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    protected void forwardElectionMessage(
        String requestId,
        int energyAmount,
        String initiatorId,
        String currentWinnerId,
        double bestBid,
        String nextPlantAddress
    ) {
        // if (nextPlantAddress == null) {
        //     // Single node - complete election immediately
        //     ringNetwork.handleElectionComplete(currentWinnerId, requestId, energyAmount);
        //     return;
        // }

        ManagedChannel channel = null;
        ElectionMessage msg = ElectionMessage.newBuilder()
            .setRequestId(requestId)
            .setEnergyRequest(energyAmount)
            .setInitiatorId(initiatorId)
            .setCurrentWinnerId(currentWinnerId)
            .setBestBid(bestBid)
            .build();

        try {

            String[] parts = nextPlantAddress.split(":");
            channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                    .usePlaintext()
                    .build();

            PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = PlantCommunicationGrpc.newBlockingStub(channel)
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


    protected void forwardGreetingMessage(String originPlantId, String originAddress, String nextPlantAddress) {
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

                PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = PlantCommunicationGrpc
                        .newBlockingStub(channel)
                        .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);

                GreetingsMessage greeting = GreetingsMessage.newBuilder()
                        .setPlantId(originPlantId) // Keep original sender
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
                    ringNetwork.recalculateRingConnections();
                }

            }
        }

    }

    protected void forwardFarewellMessage(String leavingPlantId, String prevPlantId, String nextPlantAddress) {
        FarewellMessage msg = FarewellMessage.newBuilder()
            .setPlantId(leavingPlantId)
            .setPrevPlantId(prevPlantId)
            .build();

        String[] parts = nextPlantAddress.split(":");

        ManagedChannel channel = ManagedChannelBuilder.forAddress(parts[0], Integer.parseInt(parts[1]))
                .usePlaintext()
                .build();

        PlantCommunicationGrpc.PlantCommunicationBlockingStub stub = PlantCommunicationGrpc.newBlockingStub(channel)
                .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);

        FarewellResponse response = stub.sendFarewellMessage(msg);

        if (response.getSuccess())
            System.out.println("Farewell message successfully forwarded.");

    }

}
