package plant;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.springframework.http.*;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

public class PlantClient {

    public static void main(String[] args) {
        RestTemplate client = new RestTemplate();
        String serverAddress = "http://localhost:8080";

        String postPath = "/plants/add";

        String listening = "localhost:";
        String port = "-1";
        String administration = "localhost:8080";
        String id = "-1";

        BufferedReader inFromUser = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("Insert port number: ");
        try {
            port = inFromUser.readLine();
            listening += port;
        } catch (IOException e) {
            e.printStackTrace();
        }

        PowerPlant plant = null;
        while (true){
            System.out.print("Insert Id: ");
            try {
                id = inFromUser.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            plant = new PowerPlant(id, listening, administration);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<PowerPlant> request = new HttpEntity<>(plant, headers);

            try {
                ResponseEntity<String> postResponse = client.postForEntity(serverAddress + postPath, request, String.class);

                System.out.println("POST Response: " + postResponse.getStatusCode());
                break;

            } catch (HttpClientErrorException e) {
                if (e.getStatusCode() == HttpStatus.CONFLICT)
                    System.out.println("POST Response: " + HttpStatus.CONFLICT + "\n\t" + e.getResponseBodyAsString() + "\n");
            }
        }

        // GET REQUEST (get all plants)
        String getPath = "/plants";
        ResponseEntity<PlantInfo[]> getAllResponse = client.getForEntity(serverAddress + getPath, PlantInfo[].class);
        System.out.println("GET All Response: " + getAllResponse.getStatusCode());

        PlantInfo[] plants = getAllResponse.getBody();
        if (plants != null) {
            System.out.println("Plants List:");
            for (PlantInfo p : plants) {
                System.out.println("\t" + p);
            }
            System.out.println();
        }

        plant.initializePlant(plants);

    }

}