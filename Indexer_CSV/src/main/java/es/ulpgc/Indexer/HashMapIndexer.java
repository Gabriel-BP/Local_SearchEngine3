package es.ulpgc.Indexer;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HashMapIndexer {
    private final Map<String, Set<String>> hashMapIndex;
    private final ExecutorService executor;

    public HashMapIndexer() {
        this.hashMapIndex = new ConcurrentHashMap<>();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()); // Usa el número de núcleos disponibles
    }

    public void addWord(String word, String ebookNumber) {
        executor.submit(() -> {
            hashMapIndex.computeIfAbsent(word, k -> Collections.synchronizedSet(new HashSet<>())).add(ebookNumber);
        });
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }

    public Set<String> search(String word) {
        return hashMapIndex.getOrDefault(word, Collections.emptySet());
    }

    public Map<String, Set<String>> getIndex() {
        return hashMapIndex;
    }
}
