package plant.models;

public class EnergyRequestInfo {
    private final String id;
    private final int energyAmount;
    
    public EnergyRequestInfo(String id, int energyAmount) {
        this.id = id;
        this.energyAmount = energyAmount;
    }
    
    public String getId() {
        return id;
    }
    
    public int getEnergyAmount() {
        return energyAmount;
    }
}