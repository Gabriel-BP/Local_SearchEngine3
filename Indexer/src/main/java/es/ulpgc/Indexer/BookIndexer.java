package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class BookIndexer {
    private final HashMapIndexer hashMapIndexer;
    private final Trie trie;

    public BookIndexer() {
        this.hashMapIndexer = new HashMapIndexer();
        this.trie = new Trie();
    }

    // Método para indexar los libros en paralelo
    public void indexBooks(List<Book> books) {
        // Crear un ExecutorService con un número fijo de hilos
        ExecutorService executor = Executors.newFixedThreadPool(4);

        // Lista de tareas a ejecutar en paralelo
        List<Callable<Void>> tasks = new ArrayList<>();

        // Agregar las tareas de indexación para cada libro
        for (Book book : books) {
            tasks.add(() -> {
                indexBook(book);  // Indexar el libro en paralelo
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

    // Método para indexar un libro individualmente
    public void indexBook(Book book) {
        for (String word : book.words) {
            hashMapIndexer.addWord(word, book.ebookNumber);
            trie.insert(word, book.ebookNumber);
        }
    }

    public HashMapIndexer getHashMapIndexer() {
        return hashMapIndexer;
    }

    public Trie getTrie() {
        return trie;
    }
}