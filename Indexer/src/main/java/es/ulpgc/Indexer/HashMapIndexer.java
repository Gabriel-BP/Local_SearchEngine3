package es.ulpgc.Indexer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HashMapIndexer {
    private final Map<String, Set<String>> hashMapIndex;

    public HashMapIndexer() {
        this.hashMapIndex = new HashMap<>();
    }

    // Method to add words in parallel
    public void addWord(String word, String ebookNumber) {
        // Using an ExecutorService to handle additions in parallel
        ExecutorService executor = Executors.newFixedThreadPool(4); // Number of threads is adjustable

        // Calling submit to execute the task in parallel
        executor.submit(() -> {
            synchronized (this) { // Synchronizing to protect access to the HashMap
                hashMapIndex.computeIfAbsent(word, k -> new HashSet<>()).add(ebookNumber);
            }
        });

        // Wait for all tasks to finish before continuing (optional)
        executor.shutdown();
        while (!executor.isTerminated()) {
            // Wait until all tasks are finished
        }
    }

    // Method to search for the word in the index
    public Set<String> search(String word) {
        return hashMapIndex.getOrDefault(word, new HashSet<>());
    }

    // Method to get the entire index
    public Map<String, Set<String>> getIndex() {
        return hashMapIndex;
    }
}