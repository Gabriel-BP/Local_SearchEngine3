package es.ulpgc.service;

import es.ulpgc.data.DataSource;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class QueryEngine {
    private InvertedIndex invertedIndex;

    public QueryEngine(DataSource dataSource) {
        this.invertedIndex = new InvertedIndex(dataSource);
    }

    public synchronized void reloadIndex(DataSource dataSource) {
        this.invertedIndex = new InvertedIndex(dataSource); // Rebuild index with new data
    }
    // Obtener estadísticas basadas en el tipo
    public Object getStats(String type) {
        switch (type) {
            case "word_count":
                return invertedIndex.getIndex().size();
            case "doc_count":
                return invertedIndex.getIndex().values().stream()
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet())
                        .size();
            case "top_words":
                return invertedIndex.getIndex().entrySet().stream()
                        .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
                        .limit(10)
                        .map(entry -> Map.of("word", entry.getKey(), "count", entry.getValue().size()))
                        .collect(Collectors.toList());
            default:
                throw new IllegalArgumentException("Invalid stats type");
        }
    }

    // Buscar documentos basados en palabras y filtros
    public Set<Map<String, Object>> getDocuments(String[] words, Map<String, String> filters) {
        // Buscar documentos que contengan todas las palabras
        Set<String> results = Arrays.stream(words)
                .map(invertedIndex::search)
                .reduce((set1, set2) -> {
                    set1.retainAll(set2); // Intersección
                    return set1;
                })
                .orElse(Collections.emptySet());

        // Aplicar filtros de metadatos
        return results.stream()
                .filter(doc -> {
                    Map<String, String> metadata = invertedIndex.getMetadata(doc);
                    if (filters.containsKey("from") && Integer.parseInt(metadata.get("date")) < Integer.parseInt(filters.get("from"))) {
                        return false;
                    }
                    if (filters.containsKey("to") && Integer.parseInt(metadata.get("date")) > Integer.parseInt(filters.get("to"))) {
                        return false;
                    }
                    if (filters.containsKey("author") && !metadata.get("author").equalsIgnoreCase(filters.get("author"))) {
                        return false;
                    }
                    return true;
                })
                .map(doc -> Map.of("document", doc, "metadata", invertedIndex.getMetadata(doc)))
                .collect(Collectors.toSet());
    }
}