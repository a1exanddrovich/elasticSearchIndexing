package org.example.controller;

import org.example.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/es")
public class ElasticSearchController {

    @Autowired
    private ElasticSearchService service;

    @GetMapping("/put")
    public String putData(@RequestParam(name = "name") String name) {
        return service.updateCsvData(name);
    }

}
