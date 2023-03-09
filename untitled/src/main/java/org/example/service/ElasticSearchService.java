package org.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import lombok.SneakyThrows;
import org.example.entity.Person;
import org.example.extractor.PersonExtractor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ElasticSearchService {
    private static final String INDEX_NAME = "test";

    @Autowired
    private PersonExtractor extractor;
    @Autowired
    private ElasticsearchClient client;

    public String updateCsvData(String fileName) {
        return extractor.extractByFileName(fileName)
                .stream()
                .map(this::insertOnePerson)
                .reduce((id1, id2) -> id1 + "\n" + id2)
                .orElse("No data inserted");
    }

    @SneakyThrows
    private String insertOnePerson(Person person) {
        return client.index(i -> i
                        .index(INDEX_NAME)
                        .id(person.getId())
                        .document(person)
                ).id();
    }

}
