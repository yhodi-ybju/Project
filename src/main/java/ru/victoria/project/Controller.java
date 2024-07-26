package ru.victoria.project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    @Autowired
    private DataLoadService dataLoadService;

    @Autowired
    private CsvService csvService;

    @GetMapping("/load-data")
    public String load() {
        dataLoadService.loadData();
        return "Process finished";
    }

    @GetMapping("/export")
    public String export() {
        csvService.exportDataToCsv("dm_f101_round_f.csv");
        return "Export finished";
    }

    @GetMapping("/import")
    public String importData() {
        csvService.importDataFromCsv("dm_f101_round_f.csv");
        return "Import finished";
    }
}
