package plant;

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import plant.models.PlantInfo;

public class PlantClient {
    private final String adminServerAddress;
    private final RestTemplate restTemplate = new RestTemplate();
    private final String getPath = "/plants";
    private final String postPath = "/plants/add";
    private final String deletePath = "/plants/delete";

    public PlantClient(String adminServerAddress) {
        this.adminServerAddress=adminServerAddress;
    }


    public PlantInfo[] register(PowerPlant plant) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<PowerPlant> request = new HttpEntity<>(plant, headers);
        String urlPost = "http://" + adminServerAddress + postPath;
        try {
            ResponseEntity<String> postResponse = restTemplate.postForEntity(urlPost, request, String.class);
            System.out.println("POST Response: " + postResponse.getStatusCode());

        } catch (HttpClientErrorException e) {
            if (e.getStatusCode() == HttpStatus.CONFLICT)
                System.out.println("POST Response: " + HttpStatus.CONFLICT + "\n\t" + e.getResponseBodyAsString() + "\n");
                return null;
        }

        // GET REQUEST (get all plants)
        String urlGet = "http://" + adminServerAddress + getPath;
        ResponseEntity<PlantInfo[]> getAllResponse = restTemplate.getForEntity(urlGet, PlantInfo[].class);
        System.out.println("GET All Response: " + getAllResponse.getStatusCode());

        return getAllResponse.getBody();
    }

    public boolean notifyLeaving(String plantId) {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = "http://" + adminServerAddress + deletePath + "/" + plantId;

            restTemplate.delete(url);
            System.out.println("Successfully notified admin server of shutdown.");
            return true;

        } catch (Exception e) {
            System.err.println("Error notifying admin server: " + e.getMessage());
            e.printStackTrace();
            return false;
        }

    }

}