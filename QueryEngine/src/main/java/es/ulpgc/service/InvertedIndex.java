// Clase actualizada: InvertedIndex
package es.ulpgc.service;

import es.ulpgc.data.DataSource;
import es.ulpgc.data.DatamartDataSource;

import java.util.*;
import java.util.stream.Collectors;

public class InvertedIndex {
    private final Map<String, Set<String>> index;
    private final DatamartDataSource datamartDataSource;
    private final MetadataProvider metadataProvider;

    public InvertedIndex(DataSource dataSource) {
        if (dataSource instanceof DatamartDataSource) {
            this.index = null; // No in-memory index for datamart
            this.datamartDataSource = (DatamartDataSource) dataSource;
        } else {
            this.index = dataSource.loadIndex();
            this.datamartDataSource = null;
        }
        this.metadataProvider = new MetadataProvider(dataSource);
    }

    // Devuelve el índice completo
    public Map<String, Set<String>> getIndex() {
        if (index == null) {
            throw new UnsupportedOperationException("Index is not available for DatamartDataSource");
        }
        return index;
    }

    // Busca documentos que contienen un término
    public Set<String> search(String term) {
        if (datamartDataSource != null) {
            // Search in datamart
            return datamartDataSource.searchWord(term);
        } else {
            // Search in in-memory index
            return index.getOrDefault(term, new HashSet<>());
        }
    }

    // Filtra resultados basados en metadatos
    public Set<String> filterByMultipleMetadata(Set<String> results, Map<String, String> filters) {
        if (results == null || results.isEmpty()) {
            return Collections.emptySet();
        }

        // Retrieve metadata for each result
        Map<String, Map<String, String>> metadata = metadataProvider.getMetadataForResults(results);

        // Filter results based on the metadata
        return results.stream().filter(ebookNumber -> {
            Map<String, String> ebookMetadata = metadata.get(ebookNumber);
            if (ebookMetadata == null) {
                return false; // Skip if no metadata is found
            }
            // Check each filter
            for (Map.Entry<String, String> filter : filters.entrySet()) {
                String key = filter.getKey();
                String value = filter.getValue();

                // Match metadata value with filter (partial and case-insensitive)
                if (!ebookMetadata.getOrDefault(key, "").toLowerCase().contains(value.toLowerCase())) {
                    return false; // Filter does not match
                }
            }
            return true; // All filters match
        }).collect(Collectors.toSet());
    }

    // Devuelve los metadatos de un documento específico
    public Map<String, String> getMetadata(String docId) {
        Map<String, String> metadata = metadataProvider.getMetadataForResults(Set.of(docId)).get(docId);
        if (metadata == null) {
            return Collections.emptyMap();
        }
        return metadata;
    }
}
