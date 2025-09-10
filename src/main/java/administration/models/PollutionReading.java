package administration.models;

import java.util.List;

public class PollutionReading {
    private String plantId;
    private long timestamp;
    private List<Double> averages;


    public PollutionReading(String plantId, long timestamp, List<Double> averages) {
        this.plantId=plantId;
        this.timestamp=timestamp;
        this.averages=averages;
    }


    public String getPlantId() {
        return plantId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<Double> getAverages() {
        return averages;
    }

}