package es.ulpgc.service;

import es.ulpgc.data.DatamartDataSource;
import es.ulpgc.data.DataSource;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class QueryEngine {
    private InvertedIndex invertedIndex;
    private final DataSource dataSource;

    public QueryEngine(DataSource dataSource) {
        this.dataSource = dataSource;
        this.invertedIndex = new InvertedIndex(dataSource);
    }

    private void reloadIndexIfNeeded() {
        this.invertedIndex = new InvertedIndex(dataSource);
    }

    public Object getStats(String type) {
        if (dataSource instanceof DatamartDataSource) {
            throw new UnsupportedOperationException("Stats are not supported with DatamartDataSource");
        }

        reloadIndexIfNeeded();

        switch (type) {
            case "word_count":
                return invertedIndex.getIndex().size();
            case "doc_count":
                return invertedIndex.getIndex().values().stream()
                        .flatMap(Set::stream)
                        .collect(Collectors.toSet())
                        .size();
            case "top_words":
                return getTopWords(10, 0);
            default:
                throw new IllegalArgumentException("Invalid stats type");
        }
    }

    public Set<Map<String, Object>> getDocuments(String[] words, Map<String, String> filters) {
        reloadIndexIfNeeded();

        Set<String> results = Arrays.stream(words)
                .map(invertedIndex::search)
                .reduce((set1, set2) -> {
                    set1.retainAll(set2);
                    return set1;
                })
                .orElse(Collections.emptySet());

        return results.stream()
                .filter(doc -> {
                    Map<String, String> metadata = invertedIndex.getMetadata(doc);
                    if (metadata == null || metadata.isEmpty()) return false;

                    for (Map.Entry<String, String> entry : filters.entrySet()) {
                        String key = entry.getKey();
                        String expected = entry.getValue().toLowerCase();

                        if (key.equals("from")) {
                            try {
                                int min = Integer.parseInt(expected);
                                Matcher matcher = Pattern.compile("(\\d{4})").matcher(metadata.getOrDefault("date", ""));
                                if (matcher.find()) {
                                    int year = Integer.parseInt(matcher.group(1));
                                    if (year < min) return false;
                                } else {
                                    return false;
                                }
                            } catch (Exception ignored) { }
                        } else if (key.equals("to")) {
                            try {
                                int max = Integer.parseInt(expected);
                                Matcher matcher = Pattern.compile("(\\d{4})").matcher(metadata.getOrDefault("date", ""));
                                if (matcher.find()) {
                                    int year = Integer.parseInt(matcher.group(1));
                                    if (year > max) return false;
                                } else {
                                    return false;
                                }
                            } catch (Exception ignored) { }
                        } else {
                            String field = metadata.getOrDefault(key, "").toLowerCase();
                            if (!field.contains(expected)) return false;
                        }
                    }
                    return true;
                })
                .map(doc -> Map.of("document", doc, "metadata", invertedIndex.getMetadata(doc)))
                .collect(Collectors.toSet());
    }

    public List<String> suggestWords(String prefix) {
        if (dataSource instanceof DatamartDataSource) {
            throw new UnsupportedOperationException("Autocompletion is not supported with DatamartDataSource");
        }

        reloadIndexIfNeeded();
        return invertedIndex.getIndex().keySet().stream()
                .filter(word -> word.startsWith(prefix.toLowerCase()))
                .sorted()
                .collect(Collectors.toList());
    }

    public Map<String, Object> getWordFrequency(String word) {
        reloadIndexIfNeeded();
        Set<String> docs = invertedIndex.search(word.toLowerCase());
        return Map.of("word", word, "count", docs.size());
    }

    public Map<String, String> getMetadata(String ebookNumber) {
        reloadIndexIfNeeded();
        return invertedIndex.getMetadata(ebookNumber);
    }

    public List<Map<String, Object>> getTopWords(int limit, int offset) {
        if (dataSource instanceof DatamartDataSource) {
            throw new UnsupportedOperationException("Top words are not supported with DatamartDataSource");
        }

        reloadIndexIfNeeded();
        return invertedIndex.getIndex().entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e2.getValue().size(), e1.getValue().size()))
                .skip(offset)
                .limit(limit)
                .map(e -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("word", e.getKey());
                    map.put("count", e.getValue().size());
                    return map;
                })
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getRareWords(int limit, int offset) {
        if (dataSource instanceof DatamartDataSource) {
            throw new UnsupportedOperationException("Rare words are not supported with DatamartDataSource");
        }

        reloadIndexIfNeeded();
        return invertedIndex.getIndex().entrySet().stream()
                .filter(e -> e.getValue().size() == 1)
                .skip(offset)
                .limit(limit)
                .map(e -> {
                    Map<String, Object> map = new HashMap<>();
                    map.put("word", e.getKey());
                    map.put("documents", e.getValue());
                    return map;
                })
                .collect(Collectors.toList());
    }

}
