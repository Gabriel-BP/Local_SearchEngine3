package es.ulpgc.service;

import org.springframework.web.bind.annotation.*;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/queryengine")
public class QueryController {

    private final QueryEngine queryEngine;

    public QueryController(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    // ---------------------- Estadísticas ----------------------

    @GetMapping("/stats/word_count")
    public Map<String, Object> getWordCount() {
        return Map.of("type", "word_count", "value", queryEngine.getStats("word_count"));
    }

    @GetMapping("/stats/doc_count")
    public Map<String, Object> getDocCount() {
        return Map.of("type", "doc_count", "value", queryEngine.getStats("doc_count"));
    }

    @GetMapping("/stats/top_words")
    public Map<String, Object> getTopWords(
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return Map.of(
                "type", "top_words",
                "offset", offset,
                "limit", limit,
                "value", queryEngine.getTopWords(limit, offset)
        );
    }

    @GetMapping("/stats/rare_words")
    public Map<String, Object> getRareWords(
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return Map.of(
                "type", "rare_words",
                "offset", offset,
                "limit", limit,
                "value", queryEngine.getRareWords(limit, offset)
        );
    }

    // ---------------------- Búsqueda de documentos ----------------------

    @GetMapping("/documents/{words}")
    public Map<String, Object> getDocuments(
            @PathVariable String words,
            @RequestParam(required = false) String from,
            @RequestParam(required = false) String to,
            @RequestParam(required = false) String author,
            @RequestParam(required = false) String title,
            @RequestParam(required = false) String language,
            @RequestParam(required = false) String credits,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "0") int offset
    ) {
        String[] wordArray = words.split("\\+");
        Map<String, String> filters = new HashMap<>();
        if (from != null) filters.put("from", from);
        if (to != null) filters.put("to", to);
        if (author != null) filters.put("author", author);
        if (title != null) filters.put("title", title);
        if (language != null) filters.put("language", language);
        if (credits != null) filters.put("credits", credits);

        Set<Map<String, Object>> allResults = queryEngine.getDocuments(wordArray, filters);
        List<Map<String, Object>> paginatedResults = allResults.stream()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());

        return Map.of(
                "status", "success",
                "offset", offset,
                "limit", limit,
                "total", allResults.size(),
                "results", paginatedResults
        );
    }

    // ---------------------- Autocompletado ----------------------

    @GetMapping("/words/suggest")
    public Map<String, Object> suggestWords(
            @RequestParam String prefix,
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(defaultValue = "0") int offset
    ) {
        List<String> suggestions = queryEngine.suggestWords(prefix);
        List<String> paginated = suggestions.stream()
                .skip(offset)
                .limit(limit)
                .collect(Collectors.toList());

        return Map.of(
                "prefix", prefix,
                "offset", offset,
                "limit", limit,
                "total", suggestions.size(),
                "suggestions", paginated
        );
    }

    // ---------------------- Frecuencia de palabra ----------------------

    @GetMapping("/words/{word}/frequency")
    public Map<String, Object> getWordFrequency(@PathVariable String word) {
        return queryEngine.getWordFrequency(word);
    }

    // ---------------------- Metadatos ----------------------

    @GetMapping("/metadata/{ebookNumber}")
    public Map<String, String> getMetadata(@PathVariable String ebookNumber) {
        return queryEngine.getMetadata(ebookNumber);
    }
}
