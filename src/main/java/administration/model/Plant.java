package administration.model;

public class Plant {
    private String plantId;
    private String listeningAddress;
    // private String adminServer;

    public Plant(){}

    public Plant(String id, String listeningAddress) { //, String adminServer) {
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

}