package administration.controller;

import java.util.List;
import administration.service.*;
import administration.AdministrationServer;
import administration.model.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/plants")
public class PlantsController {

    private final PlantService plantService;

    @Autowired //tells Spring to automatically resolve and inject the required bean (object) into the class where it's used
    public PlantsController(PlantService plantService) {
        this.plantService = plantService;
    }

    // Return list of plants
    @GetMapping
    public ResponseEntity<List<PlantInfo>> getPlantsList() {
        return ResponseEntity.ok(plantService.getPlantsList());
    }

    // Add a new plant
    @PostMapping("/add")
    public ResponseEntity<?> addPlant(@RequestBody PlantInfo plant) {
        if (plantService.add(plant)) return ResponseEntity.ok().build();
        else {
           return ResponseEntity
            .status(HttpStatus.CONFLICT)
            .body("ID already exists.");
        }
    }

    // Get plant by id
    @GetMapping("/get/{id}")
    public ResponseEntity<PlantInfo> getById(@PathVariable String id) {
        PlantInfo plant = plantService.getById(id);
        
        // System.out.println(id);

        if (plant != null)
            return ResponseEntity.ok(plant);
        else
            return ResponseEntity.notFound().build();
    }


    // Remove plant by id
    @DeleteMapping("/delete/{id}")
    public ResponseEntity<?> removePlant(@PathVariable("id") String plantId){
        plantService.removePlantById(plantId);
        return ResponseEntity.ok().build();
    }


    @GetMapping("/pollution/statistics")
    public ResponseEntity<Double> getPollutionStatistics(@RequestParam("t1") long t1, @RequestParam("t2") long t2) {
        double average = AdministrationServer.calculateAverageBetweenTimestamps(t1,t2);
        return ResponseEntity.ok(average);
    }
}