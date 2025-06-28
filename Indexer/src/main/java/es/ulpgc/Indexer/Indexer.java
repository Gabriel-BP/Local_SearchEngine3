package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Indexer {
    private final BookIndexer bookIndexer;

    public Indexer() {
        this.bookIndexer = new BookIndexer();
    }

    // Solo construye los índices en paralelo
    public void buildIndexes(List<Book> books) {
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Callable<Void>> tasks = new ArrayList<>();

        for (Book book : books) {
            tasks.add(() -> {
                bookIndexer.indexBook(book);
                return null;
            });
        }

        try {
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            System.err.println("Error during parallel execution: " + e.getMessage());
        } finally {
            executor.shutdown();
        }
    }

    public BookIndexer getBookIndexer() {
        return bookIndexer;
    }
}
