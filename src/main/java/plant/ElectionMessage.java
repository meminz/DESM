package plant;

public class ElectionMessage {

    private String plantId;
    private double offeredPrice;
    private final String requestId;
    private final int energyAmount;
    
    public ElectionMessage(String plantId, double offeredPrice, String requestId, int energyAmount) {
        this.plantId = plantId;
        this.offeredPrice = offeredPrice;
        this.requestId = requestId;
        this.energyAmount = energyAmount;
    }

    public String getPlantId() {
        return plantId;
    }

    public void setPlantId(String plantId) {
        this.plantId = plantId;
    }

    public double getOfferedPrice() {
        return offeredPrice;
    }

    public void setOfferedPrice(double offeredPrice) {
        this.offeredPrice = offeredPrice;
    }

    public String getRequestId() {
        return requestId;
    }

    public int getEnergyAmount() {
        return energyAmount;
    }


}
