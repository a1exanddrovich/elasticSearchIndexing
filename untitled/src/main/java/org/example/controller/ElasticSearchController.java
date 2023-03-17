package org.example.controller;

import org.example.entity.Person;
import org.example.search.SearchType;
import org.example.service.ElasticSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/es")
public class ElasticSearchController {

    @Autowired
    private ElasticSearchService service;

    @GetMapping("/put")
    public String putData(@RequestParam(name = "name") String name) {
        return service.updateCsvData(name);
    }

    @GetMapping("/search/matchQuery")
    public List<Person> matchQuery(@RequestParam(name = "query") String query,
                                   @RequestParam(name = "field") String field) {
        return service.search(SearchType.MATCH_QUERY, query, field);
    }

    @GetMapping("/search/matchBooleanQuery")
    public List<Person> matchQueryBoolean(@RequestParam(name = "query") String query,
                                          @RequestParam(name = "mandatoryField") String mandatoryField,
                                          @RequestParam(name = "secondaryField") String secondaryField) {
        return service.search(SearchType.MATCH_QUERY_BOOLEAN, query, mandatoryField, secondaryField);
    }

    @GetMapping("/search/matchPhraseQuery")
    public List<Person> matchPhraseQuery(@RequestParam(name = "query") String query,
                                         @RequestParam(name = "field") String field) {
        return service.search(SearchType.MATCH_PHRASE_QUERY, query, field);
    }

    @GetMapping("/search/fuzzyQuery")
    public List<Person> fuzzyQuery(@RequestParam(name = "query") String query,
                                   @RequestParam(name = "field") String field,
                                   @RequestParam(name = "level", defaultValue = "auto", required = false) String level) {
        return service.search(SearchType.FUZZY_QUERY, query, field, level);
    }

    @GetMapping("/search/mltQuery")
    public List<Person> moreLikeThisQuery(@RequestParam(name = "query") String query,
                                          @RequestParam(name = "field1") String field1,
                                          @RequestParam(name = "field2") String field2,
                                          @RequestParam(name = "minTermFreq", defaultValue = "0", required = false) String minTermFreq,
                                          @RequestParam(name = "minDocFreq", defaultValue = "0", required = false) String minDocFreq) {
        return service.search(SearchType.MORE_LIKE_THIS_QUERY, query, field1, field2, minTermFreq, minDocFreq);
    }

    @GetMapping("/search/percolateQuery")
    public List<Person> percolateQuery(@RequestParam(name = "age") String age,
                                       @RequestParam(name = "name") String name,
                                       @RequestParam(name = "department") String department) {
        return service.search(SearchType.PERCOLATE_QUERY, null, age, name, department);
    }

    @GetMapping("/search/scriptQuery")
    public List<Person> scriptQuery(@RequestParam(name = "id") String id,
                                    @RequestParam(name = "ageValue") String ageValue) {
        return service.search(SearchType.SCRIPT_QUERY, null, id, ageValue);
    }

    @GetMapping("/search/geoQuery")
    public List<Person> geoQuery(@RequestParam(name = "distance") String distance,
                                 @RequestParam(name = "lat") String lat,
                                 @RequestParam(name = "lon") String lon) {
        return service.search(SearchType.GEO_QUERY, null, distance, lat, lon);
    }

}
