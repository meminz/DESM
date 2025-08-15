package plant;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

public class PlantClient {

    public static void main(String[] args) {
        String id = "-1";

        RestTemplate client = new RestTemplate();
        String serverAddress = "http://localhost:8080";

        String postPath = "/plants/add";

        String listening = "localhost:1111";
        String administration = "localhost:8080";

        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Insert Id: ");

        try {
            id = inFromUser.readLine();
        } catch (Exception e) {
            e.printStackTrace();
        }

        PowerPlant plant = new PowerPlant(id, listening, administration);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<PowerPlant> request = new HttpEntity<>(plant, headers);

        try {
            ResponseEntity<String> postResponse = client.postForEntity(serverAddress + postPath, request, String.class);

            System.out.println("POST Response: " + postResponse.getStatusCode());

        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT)
                System.out.println("POST Response: " + HttpStatus.CONFLICT + "\n\t" + e.getResponseBodyAsString());
        }

        // GET REQUEST (get all plants)
        String getPath = "/plants";
        ResponseEntity<PowerPlant[]> getAllResponse = client.getForEntity(serverAddress + getPath, PowerPlant[].class);
        System.out.println("GET All Response: " + getAllResponse.getStatusCode());

        PowerPlant[] plants = getAllResponse.getBody();
        if (plants != null) {
            System.out.println("Plants List:");
            for (PowerPlant p : plants) {
                System.out.println("\t" + p);
            }
            System.out.println();
        }

        plant.initializePlant();


    }
    
}