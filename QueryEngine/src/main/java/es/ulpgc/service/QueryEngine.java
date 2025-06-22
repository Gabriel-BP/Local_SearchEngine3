package es.ulpgc.service;

import es.ulpgc.data.DataSource;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class QueryEngine {
    private InvertedIndex invertedIndex;
    private DataSource dataSource;

    public QueryEngine(DataSource dataSource) {
        this.dataSource = dataSource;
        this.invertedIndex = new InvertedIndex(dataSource); // Cargamos el índice al iniciar
    }

    // Recargar el índice cuando sea necesario
    private void reloadIndexIfNeeded() {
        // Lógica para verificar si los datos en MinIO han cambiado y recargar el índice
        // Por ejemplo, puedes basarte en una marca de tiempo o alguna señal externa
        this.invertedIndex = new InvertedIndex(dataSource); // Forzamos la recarga de los datos
    }

    public Object getStats(String type) {
        reloadIndexIfNeeded(); // Aseguramos que los datos estén siempre actualizados

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

    public Set<Map<String, Object>> getDocuments(String[] words, Map<String, String> filters) {
        reloadIndexIfNeeded(); // Aseguramos que los datos estén siempre actualizados

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
