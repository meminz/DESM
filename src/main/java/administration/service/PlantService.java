package administration.service;
import administration.model.Plant;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service //Spring automatically makes this class a singleton bean
public class PlantService {

    private final List<Plant> plantsList = new ArrayList<>();
    private final Set<String> idsList = new HashSet<>();

    public synchronized List<Plant> getPlantsList() {
        return new ArrayList<>(plantsList);
    }

    public synchronized void setPlantsList(List<Plant> plants) {
        plantsList.clear();
        plantsList.addAll(plants);
    }

    private synchronized boolean idAlreadyExists(String id){
        return idsList.contains(id);
    }

    public boolean add(Plant plant){
        if (idAlreadyExists(plant.getId())){
            return false;
        } else {
            plantsList.add(plant);
            idsList.add(plant.getId());
            return true;
        }
    }


    public synchronized Plant getById(String id) {
        System.out.println("called");
        for (Plant plant : plantsList) {

            // System.out.println(plant.getId());
            // System.out.println(id);

            if (plant.getId().equals("-1") && plant.getId().equals(id)) {
                // System.out.println(plant);
                return plant;
            }
        }
        return null; 
    }

    public synchronized void removePlantById(String id){
        plantsList.removeIf(p -> p.getId().equals(id));
    }
}
