package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

public class Indexer {
    private final BookIndexer bookIndexer;
    private final DataMartWriter dataMartWriter;

    // Constructor
    public Indexer() {
        this.bookIndexer = new BookIndexer();
        this.dataMartWriter = new DataMartWriter();
    }

    // Método para indexar los libros en paralelo utilizando ExecutorService
    public void buildIndexes(List<Book> books) {
        // Crear un ExecutorService con un número fijo de hilos
        ExecutorService executor = Executors.newFixedThreadPool(4);  // Ajusta el número de hilos según tu sistema

        // Lista de tareas a ejecutar en paralelo
        List<Callable<Void>> tasks = new ArrayList<>();

        // Agregar las tareas de indexación para cada libro
        for (Book book : books) {
            tasks.add(() -> {
                bookIndexer.indexBook(book);  // Indexar el libro en paralelo
                return null;
            });
        }

        try {
            // Ejecutar todas las tareas en paralelo usando invokeAll
            executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            System.err.println("Error during parallel execution: " + e.getMessage());
        } finally {
            // Apagar el ExecutorService después de completar las tareas
            executor.shutdown();
        }
    }

    // Método para indexar los libros y escribir los resultados
    public void indexBooks(List<Book> books, String outputType) {
        try {
            buildIndexes(books);  // Indexar los libros en paralelo

            // Escribir los resultados en el formato deseado (CSV o DataMart)
            switch (outputType.toLowerCase()) {
                case "datamart":
                    dataMartWriter.saveMetadataToDataMart(books);
                    dataMartWriter.saveContentToDataMart(bookIndexer.getHashMapIndexer().getIndex());
                    break;
                default:
                    System.err.println("Unsupported output type: " + outputType);
            }
        } catch (Exception e) {
            dataMartWriter.saveContentToDataMart(bookIndexer.getHashMapIndexer().getIndex());
            System.err.println("Error during indexing: " + e.getMessage());
        }
    }
}
