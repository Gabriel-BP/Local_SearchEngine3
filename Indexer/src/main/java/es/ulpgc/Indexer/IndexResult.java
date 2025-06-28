package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

public class IndexResult implements Serializable {
    public Book book;
    public Map<String, Set<String>> wordIndex;

    public IndexResult(Book book, Map<String, Set<String>> wordIndex) {
        this.book = book;
        this.wordIndex = wordIndex;
    }
}
