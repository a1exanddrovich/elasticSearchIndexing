package org.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.LatLonGeoLocation;
import co.elastic.clients.elasticsearch._types.StoredScriptId;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import lombok.SneakyThrows;
import org.example.entity.Person;
import org.example.extractor.PersonExtractor;
import org.example.search.SearchType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


@Service
public class ElasticSearchService {
    private static final String INDEX_NAME = "test";
    private static final String PERCOLATE_INDEX_NAME = "percolateful_index";
    private static final String SCRIPT_PARAM_NAME = "ageParam";

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
    public List<Person> search(SearchType searchType, String... data) {
        Function<Query.Builder, ObjectBuilder<Query>> builtQuery = switch (searchType) {
            case MATCH_QUERY -> buildMatchQuery(data[0], data[1]);
            case MATCH_QUERY_BOOLEAN -> buildMatchBooleanQuery(data[0], data[1], data[2]);
            case MATCH_PHRASE_QUERY -> buildMatchPhraseQuery(data[0], data[1]);
            case MORE_LIKE_THIS_QUERY -> buildMoreLikeThisQuery(data[0], data[1], data[2], data[3], data[4]);
            case FUZZY_QUERY -> buildFuzzyQuery(data[0], data[1], data[2]);
            case SCRIPT_QUERY -> buildScriptQuery(data[0], data[1]);
            case GEO_QUERY -> buildGeoQuery(data[0], data[1], data[2]);
            case PERCOLATE_QUERY -> buildPercolateQuery(data[0], data[1], data[2]);
        };

        SearchResponse<Person> response = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(builtQuery),
                Person.class
        );

        return response
                .hits()
                .hits()
                .stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchQuery(String queryString, String field) {
        return builder -> builder
                .match(query -> query
                        .field(field)
                        .query(queryString)
                );
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchBooleanQuery(String queryString, String firstField, String secondField) {
        Query byMandatoryField = MatchQuery.of(m -> m.field(firstField).query(queryString))._toQuery();
        Query bySecondaryField = MatchQuery.of(m -> m.field(secondField).query(queryString))._toQuery();

        return builder -> builder
                .bool(query -> query
                        .must(byMandatoryField)
                        .should(bySecondaryField)
                );
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchPhraseQuery(String queryString, String field) {
        return builder -> builder
                .matchPhrase(phrase -> phrase
                        .field(field)
                        .query(queryString)
                );
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildFuzzyQuery(String query, String field, String level) {
        return builder -> builder
                .fuzzy(fuzzyQuery -> fuzzyQuery
                        .field(field)
                        .fuzziness(level)
                        .value(query)
                );
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMoreLikeThisQuery(String query,
                                                                                 String firstField,
                                                                                 String secondField,
                                                                                 String minTermFrequency,
                                                                                 String minDocFrequency) {
        return builder -> builder
                .moreLikeThis(mltQuery -> mltQuery
                        .like(text -> text.text(query))
                        .fields(List.of(firstField, secondField))
                        .minTermFreq(Integer.parseInt(minTermFrequency))
                        .minDocFreq(Integer.parseInt(minDocFrequency))
                );

    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildPercolateQuery(String age, String name, String department) {
        return builder -> builder
                .percolate(percolateQuery -> percolateQuery
                        .index(PERCOLATE_INDEX_NAME)
                        .document(JsonData.of(Person.builder()
                                .age(Integer.parseInt(age))
                                .name(name)
                                .department(department)
                                .build()))
                );
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildScriptQuery(String scriptId, String ageValue) {
        return builder -> builder
                .script(script ->
                        script.script(
                                scriptBuilder -> scriptBuilder
                                        .stored(StoredScriptId.of(scriptIdBuilder -> scriptIdBuilder
                                                .id(scriptId)
                                                .params(Map.of(SCRIPT_PARAM_NAME, JsonData.of(ageValue))))
                                        )
                        )
                );

    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildGeoQuery(String distanceString, String lat, String lon) {
        return builder -> builder
                .geoDistance(distance -> distance
                        .distance(distanceString)
                        .field("personLocation")
                        .location(GeoLocation.of(e ->
                                        e.latlon(LatLonGeoLocation.of(h -> h
                                                .lat(Double.parseDouble(lat))
                                                .lon(Double.parseDouble(lon)))
                                        )
                                )
                        )
                );
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
