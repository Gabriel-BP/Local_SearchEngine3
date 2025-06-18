package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Indexer {
    private final BookIndexer bookIndexer;
    private final CSVWriter csvWriter;
    private final ExecutorService executor;

    public Indexer() {
        this.bookIndexer = new BookIndexer();
        this.csvWriter = new CSVWriter();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void buildIndexes(List<Book> books) {
        CompletionService<Void> completionService = new ExecutorCompletionService<>(executor);

        for (Book book : books) {
            completionService.submit(() -> {
                bookIndexer.indexBook(book);
                return null;
            });
        }

        try {
            for (int i = 0; i < books.size(); i++) {
                completionService.take().get(); // Esperar a que todas las tareas terminen
            }
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error en la construcción del índice: " + e.getMessage());
        }
    }

    public void indexBooks(List<Book> books, String outputType) {
        try {
            buildIndexes(books);

            switch (outputType.toLowerCase()) {
                case "csv":
                    csvWriter.saveMetadataToCSV(books);
                    csvWriter.saveContentToCSV(bookIndexer.getHashMapIndexer().getIndex());
                    break;
                default:
                    System.err.println("Unsupported output type: " + outputType);
            }
        } catch (Exception e) {
            System.err.println("Error durante la indexación: " + e.getMessage());
        }
    }

    public void shutdown() {
        executor.shutdown();
        bookIndexer.shutdown();
    }
}
