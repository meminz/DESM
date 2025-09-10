package plant.sensors;

import java.util.ArrayList;
import java.util.List;
import Simulators.Buffer;
import Simulators.Measurement;

public class SensorBuffer implements Buffer {
    List<Measurement> measurements = new ArrayList<>();

    public synchronized void addMeasurement(Measurement m){
        measurements.add(m);
    }

    public synchronized List<Measurement> readAllAndClean(){
        List<Measurement> ret = new ArrayList<>(measurements);
        measurements.clear();
        return ret;
    }

}
