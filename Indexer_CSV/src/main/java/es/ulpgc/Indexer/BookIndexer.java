package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BookIndexer {
    private final HashMapIndexer hashMapIndexer;
    private final Trie trie;
    private final ExecutorService executor;

    public BookIndexer() {
        this.hashMapIndexer = new HashMapIndexer();
        this.trie = new Trie();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void indexBooks(List<Book> books) {
        List<Future<Void>> futures = new ArrayList<>();
        for (Book book : books) {
            futures.add(executor.submit(() -> {
                indexBook(book);
                return null;
            }));
        }

        // Esperar a que todas las tareas terminen
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                System.err.println("Error en la indexación: " + e.getMessage());
            }
        }
    }

    public void indexBook(Book book) {
        for (String word : book.words) {
            hashMapIndexer.addWord(word, book.ebookNumber);
            trie.insert(word, book.ebookNumber);
        }
    }

    public void shutdown() {
        executor.shutdown();
        hashMapIndexer.shutdown();
    }

    public HashMapIndexer getHashMapIndexer() {
        return hashMapIndexer;
    }

    public Trie getTrie() {
        return trie;
    }
}
