package plant.models;

import java.util.Objects;

public class PlantInfo {
    private String plantId;
    private String listeningAddress;
    // private String adminServer;

    public PlantInfo(){}

    public PlantInfo(String id, String listeningAddress) { //, String adminServer) {
        this.plantId = id;
        this.listeningAddress = listeningAddress;
        // this.adminServer = adminServer;
    }

    public String getId() {
        return plantId;
    }

    public void setId(String plantId) {
        this.plantId = plantId;
    }

    public String getListeningAddress() {
        return listeningAddress;
    }

    public void setListeningAddress(String listeningAddress) {
        this.listeningAddress = listeningAddress;
    }

    @Override
    public String toString() {
        return "Id: " + plantId + ", listening on: " + listeningAddress;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        if (obj.getClass().equals("".getClass()))
            return this.plantId.equals((String)obj);

        PlantInfo other = (PlantInfo)obj;
        return this.plantId == other.getId();
    }

    @Override
    public int hashCode() {
        return Objects.hash(plantId);
    }

}