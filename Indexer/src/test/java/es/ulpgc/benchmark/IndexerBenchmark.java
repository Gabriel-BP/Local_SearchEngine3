package es.ulpgc.benchmark;

import es.ulpgc.Cleaner.Book;

import es.ulpgc.Indexer.HashMapIndexer;
import es.ulpgc.Indexer.Trie;
import es.ulpgc.Indexer.Indexer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Setup;

import java.util.ArrayList;
import java.util.List;

@State(org.openjdk.jmh.annotations.Scope.Thread)
public class IndexerBenchmark {

    private Indexer indexer;
    private List<Book> books;

    @Setup
    public void setUp() {

        books = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            List<String> words = new ArrayList<>();
            for (int j = 0; j < 100; j++) {
                words.add("word" + j);
            }
            books.add(new Book("Book" + i, "Author" + i, "2025", "English", "Credits", "ebook" + i, words, ""));
        }
        indexer = new Indexer();
    }

    @Benchmark
    public void benchmarkHashMapIndexer() {
        HashMapIndexer hashMapIndexer = new HashMapIndexer();
        for (Book book : books) {
            for (String word : book.words) {
                hashMapIndexer.addWord(word, book.ebookNumber);
            }
        }
    }

    @Benchmark
    public void benchmarkTrieIndexer() {
        Trie trie = new Trie();
        for (Book book : books) {
            for (String word : book.words) {
                trie.insert(word, book.ebookNumber);
            }
        }
    }

    @Benchmark
    public void benchmarkIndexer() {
        indexer.buildIndexes(books);
    }
}
