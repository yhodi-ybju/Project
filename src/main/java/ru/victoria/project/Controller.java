package ru.victoria.project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

    @Autowired
    private Service service;

    @GetMapping("/load-data")
    public String load() {
        service.loadData();
        return "Process finished";
    }
}
