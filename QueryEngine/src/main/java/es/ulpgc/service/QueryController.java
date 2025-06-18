package es.ulpgc.service;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

@RestController
@RequestMapping("/queryengine")
public class QueryController {

    private final QueryEngine queryEngine;

    public QueryController(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    @GetMapping("/stats/{type}")
    public Map<String, Object> getStats(@PathVariable String type) {
        try {
            Object stats = queryEngine.getStats(type);
            return Map.of("type", type, "value", stats);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
    }

    @GetMapping("/documents/{words}")
    public Map<String, Object> getDocuments(
            @PathVariable String words,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String author) {

        String[] wordArray = words.split("\\+");
        Map<String, String> filters = new HashMap<>();
        if (from != null) filters.put("from", from);
        if (to != null) filters.put("to", to);
        if (author != null) filters.put("author", author);

        Set<Map<String, Object>> results = queryEngine.getDocuments(wordArray, filters);
        return Map.of("status", "success", "results", results);
    }
}
