package org.example.controller;

import org.example.entity.Movie;
import org.example.search.SearchType;
import org.example.search.SearchTypeAggregated;
import org.example.service.MovieService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/movies")
public class MovieController {

    @Autowired
    private MovieService service;

    @GetMapping("/matchPhrase")
    public List<Movie> searchByMatchPhrase(@RequestParam("field") String field,
                                           @RequestParam("title") String title) {
        return service.search(SearchType.MATCH_PHRASE_QUERY, field, title);
    }

    @GetMapping("/fuzzy")
    public List<Movie> searchWithFuzziness(@RequestParam("field") String field,
                                           @RequestParam("title") String title) {
        return service.search(SearchType.FUZZY_QUERY, field, title);
    }

    @GetMapping("/avgRating")
    public List<Movie> searchByAvgRating() {
        return service.searchAggregated(SearchTypeAggregated.AVG);
    }

    @GetMapping("/filterAvgRatingWith")
    public List<Movie> searchByAvgRating(@RequestParam(value = "avgRating") String avgRating) {
        return service.searchAggregated(SearchTypeAggregated.AVG_WITH_FILTER, avgRating);
    }

    @GetMapping("/findTop")
    public List<Movie> findTopN(@RequestParam(value = "n") String n) {
        return service.searchAggregated(SearchTypeAggregated.TOP_N, n);
    }
    @GetMapping("/userRating")
    public List<Movie> userRating(@RequestParam(value = "userId") String userId) {
        return service.searchAggregated(SearchTypeAggregated.USER_RATING, userId);
    }

}
