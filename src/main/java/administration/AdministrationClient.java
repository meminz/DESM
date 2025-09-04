package administration;

import java.util.Map;
import java.util.Scanner;

import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import administration.model.PlantInfo;


public class AdministrationClient {

    private static final String ADMIN_SERVER_URL = "http://localhost:8080/";
    private static RestTemplate restTemplate = new RestTemplate();
    private static Scanner scanner = new Scanner(System.in);
    
    private static final Map<Integer, Runnable> menu = Map.of( // Map.ofEntries for more than 10
        0, () -> exit(),
        1, () -> listCurrentPlants(),
        2, () -> getPollutionStatistics()
    );

    public static void main(String[] args) {
        System.out.println("=== Administration Client ===");
        System.out.println("Connected to server: " + ADMIN_SERVER_URL);
        
        while (true) {
            showMenu();
            int choice = getUserChoice();
            
            Runnable action = menu.get(choice);
            if (action != null)
                action.run();
            else
                System.out.println("Invalid choice. Please try again.");
            
            System.out.println();
        }
    }
    
    private static void showMenu() {
        System.out.println("\n--- Administration Menu ---");
        System.out.println("0. Exit");
        System.out.println("1. List current thermal power plants");
        System.out.println("2. Get CO2 pollution statistics");
        System.out.print("Choose an option: ");
    }

    private static int getUserChoice() {
        try {
            return Integer.parseInt(scanner.nextLine().trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }


    private static void listCurrentPlants() {
        String getPath = "/plants";
        ResponseEntity<PlantInfo[]> getPlantsResponse = restTemplate.getForEntity(ADMIN_SERVER_URL + getPath, PlantInfo[].class);
        // System.out.println("GET All Response: " + getPlantsResponse.getStatusCode());

        if (getPlantsResponse.getStatusCode().is2xxSuccessful()) {
            System.out.println("\n=== Current Plants in the Network ===");
            
            for (PlantInfo p : getPlantsResponse.getBody()){
                System.out.println(p);
            }
        } else {
            System.out.println("Error retrieving plants: " + getPlantsResponse.getStatusCode());
        }
    }

    private static void getPollutionStatistics() {
        try {
            System.out.println("Current timestamp for reference: " + System.currentTimeMillis());

            System.out.print("Enter start timestamp (millisecs): ");
            long t1 = Long.parseLong(scanner.nextLine().trim());
            
            System.out.print("Enter end timestamp (millisecs): ");
            long t2 = Long.parseLong(scanner.nextLine().trim());
            
            String getPath = "plants/pollution/statistics?t1=" + t1 + "&t2=" + t2;
            
            ResponseEntity<String> getStatisticsResponse = restTemplate.getForEntity(ADMIN_SERVER_URL + getPath, String.class);
 
            if (getStatisticsResponse.getStatusCode().is2xxSuccessful()) {
                System.out.println("\n=== CO2 Statistics ===");
                System.out.println("Average CO2 emissions between " + t1 + " and " + t2 + ":");
                System.out.println(getStatisticsResponse.getBody() + " grams");
            } else {
                System.out.println("Error retrieving statistics: " + getStatisticsResponse.getStatusCode());
            }
            
        } catch (NumberFormatException e) {
            System.out.println("Invalid timestamp format. Please enter numbers only.");
        } catch (Exception e) {
            System.err.println("Error connecting to server: " + e.getMessage());
        }
    }

    private static void exit() {
        System.out.println("Goodbye!");
        System.exit(0);
    }

}