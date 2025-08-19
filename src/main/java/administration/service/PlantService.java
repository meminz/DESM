package administration.service;
import administration.model.PlantInfo;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service //Spring automatically makes this class a singleton bean
public class PlantService {

    private final List<PlantInfo> plantsList = new ArrayList<>();
    private final Set<String> idsList = new HashSet<>();

    public synchronized List<PlantInfo> getPlantsList() {
        return new ArrayList<>(plantsList);
    }

    public synchronized void setPlantsList(List<PlantInfo> plants) {
        plantsList.clear();
        plantsList.addAll(plants);
    }

    private synchronized boolean idAlreadyExists(String id){
        return idsList.contains(id);
    }

    public boolean add(PlantInfo plant){
        if (idAlreadyExists(plant.getId())){
            return false;
        } else {
            plantsList.add(plant);
            idsList.add(plant.getId());
            return true;
        }
    }


    public synchronized PlantInfo getById(String id) {
        for (PlantInfo plant : plantsList) {
            if (plant.getId().equals(id))
                return plant;
        }
        return null; 
    }

    public synchronized void removePlantById(String id){
        plantsList.removeIf(p -> p.getId().equals(id));
        idsList.removeIf(p -> p.equals(id));
    }
}
