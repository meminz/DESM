package plant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Scanner;

import plant.models.PlantInfo;

public class PlantApp {
    static PowerPlant plant;
    static final String adminServerAddress = "localhost:8080";

    public static void main(String[] args) {
        PlantClient plantClient = new PlantClient(adminServerAddress);

        String listening = "localhost:";
        int port = -1;
        String id = "-1";
        PlantInfo[] otherPlants = null;

        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        
        while (true) {
            System.out.print("Insert port number: ");
            
            try {
                port = Integer.parseInt(inFromUser.readLine());

                if ((int)Math.log10(port) + 1 == 4) break;
                else throw new NumberFormatException();

            } catch (IOException e) {
                e.printStackTrace();
            } catch (NumberFormatException ne) {
                System.out.println("You must insert a 4 digit integer number. Try again.");
            }

        }

        listening += port;

        while (true) {
            System.out.print("Insert Id: ");
            try {
                id = inFromUser.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            plant = new PowerPlant(id, listening, adminServerAddress);

            System.out.println("NEW PLANT: " + plant);

            otherPlants = plantClient.register(plant);
            if (otherPlants != null)
                break;
        }

        System.out.println("PLANT: " + plant);

        plant.initializePlant(otherPlants);
        startStdinListener();
    }


    private static final Map<String, Runnable> actions = Map.of(
        "exit", () -> exit()
    );


    private static void startStdinListener() {
        Thread stdinThread = new Thread(() -> {

            Scanner scanner = new Scanner(System.in);
            while (!Thread.currentThread().isInterrupted()) {
                String input = scanner.nextLine().trim().toLowerCase();

                Runnable command = actions.get(input);
                if (command != null) {
                    command.run();
                }

            }
            scanner.close();
        });

        stdinThread.setDaemon(true);
        stdinThread.start();
    }

    private static void exit() {
        System.out.println("Shutting down plant " + plant.getId() + "...");
        new Thread(() -> {
            plant.shutdown();
        }).run();
        
        Thread.currentThread().interrupt();
    }


}
