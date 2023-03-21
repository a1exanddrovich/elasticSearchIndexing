package org.example.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.util.ObjectBuilder;
import lombok.SneakyThrows;
import org.example.entity.Movie;
import org.example.search.SearchType;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.example.search.SearchTypeAggregated;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class MovieService {

    private static final String INDEX_NAME = "my_movies";
    private static final String FUZZINESS = "auto";

    @Autowired
    private ElasticsearchClient client;

    @SneakyThrows
    public List<Movie> search(SearchType type, String field, String data) {
        var builtQuery = switch (type) {
            case MATCH_PHRASE_QUERY -> buildMatchPhraseQuery(field, data);
            case FUZZY_QUERY -> buildFuzzyQuery(field, data);
        };

        SearchResponse<Movie> response = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(builtQuery),
                Movie.class
        );

        return response
                .hits()
                .hits()
                .stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }

    public List<Movie> searchAggregated(SearchTypeAggregated type, String... data) {
        SearchResponse<Movie> response = switch (type) {
            case AVG -> buildAggregated();
            case AVG_WITH_FILTER -> buildAggregatedWithFilter(Integer.parseInt(data[0]));
            case TOP_N -> buildAggregatedWithTopN(Integer.parseInt(data[0]));
            case USER_RATING -> buildAggregatedWithUserRating(Integer.parseInt(data[0]));
        };

        return response
                .hits()
                .hits()
                .stream()
                .map(Hit::source)
                .collect(Collectors.toList());
    }


    @SneakyThrows
    private SearchResponse<Movie> buildAggregatedWithFilter(int val) {
        return client.search(request -> request
                .size(0)
                .aggregations("by_movie", ag ->
                        ag.terms(term -> term
                                        .field("title.keyword")
                                        .size(10))
                                .aggregations("avg-rating", avgAg ->
                                        avgAg.children(children -> children.type("rating"))
                                                .aggregations("average-rating", avgRating -> avgRating.avg(field -> field.field("rating"))))
                ).postFilter(filter -> filter
                        .term(term -> term
                                .field("average-rating")
                                .value(val))
                ), Movie.class);
    }

    @SneakyThrows
    private SearchResponse<Movie> buildAggregated() {
        return client.search(request -> request
                .size(0)
                .aggregations("by_movie", ag ->
                        ag.terms(term -> term
                                        .field("title.keyword")
                                        .size(10))
                                .aggregations("avg-rating", rating ->
                                        rating.children(q -> q.type("rating"))
                                                .aggregations("average-rating", avgRating -> avgRating.avg(field -> field.field("rating"))))
                ), Movie.class);
    }

    @SneakyThrows
    private SearchResponse<Movie> buildAggregatedWithTopN(int n) {
        return client.search(r -> r
                .size(0)
                .aggregations("top_tag", ag -> ag
                        .terms(term -> term.field("tag"))
                        .aggregations("top_hits", topHit -> topHit
                                .topHits(top -> top.size(n)
                                        .sort(sort -> sort.field(field -> field.field("tag").order(SortOrder.Desc)))))), Movie.class);
    }

    @SneakyThrows
    private SearchResponse<Movie> buildAggregatedWithUserRating(int userId) {
        return client.search(request -> request
                .size(0)
                .aggregations("user-rating-filtered", ag -> ag
                        .filter(filter -> filter.term(term -> term.field("rating")
                                .value(5)))
                        .aggregations("user-rating", userRating -> userRating
                                .terms(term -> term.field("userId")
                                        .size(10))
                                .aggregations("rating", ratingAg -> ratingAg
                                        .terms(term -> term.field("rating")
                                                .size(10))
                                ))).postFilter(filter -> filter
                        .term(term -> term
                                .field("userId")
                                .value(userId))
                ), Movie.class);
    }


    private Function<Query.Builder, ObjectBuilder<Query>> buildFuzzyQuery(String field, String data) {
        return builder -> builder
                .fuzzy(fuzzyQuery -> fuzzyQuery
                        .field(field)
                        .fuzziness(FUZZINESS)
                        .value(data)
                );
    }

    private Function<Query.Builder, ObjectBuilder<Query>> buildMatchPhraseQuery(String field, String data) {
        return builder -> builder
                .matchPhrase(phrase -> phrase
                        .field(field)
                        .query(data)
                );
    }

}
