package es.ulpgc.benchmark;

import es.ulpgc.Cleaner.Book;
import es.ulpgc.Indexer.HashMapIndexer;
import es.ulpgc.Indexer.Trie;
import es.ulpgc.Indexer.Indexer;
import es.ulpgc.Cleaner.Cleaner;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Setup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@State(org.openjdk.jmh.annotations.Scope.Thread)
public class IndexerCSVBenchmark {

    private Indexer indexer;
    private List<Book> books;

    @Setup
    public void setUp() throws IOException {
        // Create test books similar to the second example
        books = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            List<String> words = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                words.add("word" + j);
            }
            books.add(new Book("Book" + i, "Author" + i, "2025", "English", "Credits", "ebook" + i, words, ""));
        }
        indexer = new Indexer();  // Use the Indexer for benchmarking
    }

    @Benchmark
    public void benchmarkIndexerCSV() {
        // Call indexBooks to save the results in CSV format
        indexer.indexBooks(books, "csv");
    }

    @Benchmark
    public void benchmarkHashMapIndexer() {
        // Benchmark HashMapIndexer performance
        HashMapIndexer hashMapIndexer = new HashMapIndexer();
        for (Book book : books) {
            for (String word : book.words) {
                hashMapIndexer.addWord(word, book.ebookNumber);
            }
        }
    }

    @Benchmark
    public void benchmarkTrieIndexer() {
        // Benchmark Trie performance
        Trie trie = new Trie();
        for (Book book : books) {
            for (String word : book.words) {
                trie.insert(word, book.ebookNumber);
            }
        }
    }

    @Benchmark
    public void benchmarkIndexer() {
        // Call buildIndexes for benchmarking the parallel indexer
        indexer.buildIndexes(books);
    }
}
