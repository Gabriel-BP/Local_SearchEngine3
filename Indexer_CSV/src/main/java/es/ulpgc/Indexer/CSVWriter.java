package es.ulpgc.Indexer;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import es.ulpgc.Cleaner.Book;

import java.io.*;
import java.util.*;

public class CSVWriter {
    private static final String INDEX_METADATA_FILE = "index_metadata.csv";
    private static final String INDEX_CONTENT_FILE = "index_content.csv";
    private static final int LOCK_TIMEOUT_SECONDS = 120;
    private static final int RETRY_DELAY_MS = 2000;

    public void saveMetadataToCSV(Iterable<Book> books) {
        FencedLock lock = waitForFencedLock("csv-lock-metadata");
        lock.lock();
        try {
            // Descargar versión actual desde MinIO
            try {
                MinioClientHelper.downloadFile("index/" + INDEX_METADATA_FILE, INDEX_METADATA_FILE);
            } catch (Exception e) {
                System.err.println("[WARN] No existe aún " + INDEX_METADATA_FILE + " en MinIO, se creará nuevo.");
                new File(INDEX_METADATA_FILE).delete(); // Asegurar limpieza previa
            }

            Set<String> existing = new HashSet<>();
            List<String> allLines = new ArrayList<>();

            // Leer metadatos existentes
            File file = new File(INDEX_METADATA_FILE);
            if (file.exists() && file.length() > 0) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    String header = reader.readLine();
                    if (header != null) allLines.add(header);

                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(",", 2);
                        if (parts.length > 0) {
                            existing.add(parts[0].trim().toLowerCase());
                            allLines.add(line);
                        }
                    }
                }
            } else {
                allLines.add("EbookNumber,Title,Author,Date,Language,Credits");
            }

            // Añadir nuevos libros (sin duplicar)
            for (Book book : books) {
                String id = book.ebookNumber.trim().toLowerCase();
                if (!existing.contains(id)) {
                    String row = String.format("%s,%s,%s,%s,%s,%s",
                            book.ebookNumber, book.title, book.author,
                            book.date, book.language, book.credits);
                    allLines.add(row);
                    existing.add(id);
                }
            }

            // Reescribir el archivo completo
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                for (String line : allLines) writer.write(line + "\n");
            }

            MinioClientHelper.uploadFile(INDEX_METADATA_FILE, "index/" + INDEX_METADATA_FILE);
        } catch (Exception e) {
            System.err.println("Error durante escritura de metadatos: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    public void saveContentToCSV(Map<String, Set<String>> wordToEbookNumbers) {
        FencedLock lock = waitForFencedLock("csv-lock-content");
        lock.lock();
        try {
            try {
                MinioClientHelper.downloadFile("index/" + INDEX_CONTENT_FILE, INDEX_CONTENT_FILE);
            } catch (Exception e) {
                System.err.println("[WARN] No existe aún " + INDEX_CONTENT_FILE + " en MinIO, se creará nuevo.");
                new File(INDEX_CONTENT_FILE).delete(); // Asegurar limpieza
            }

            // Leer líneas existentes
            Map<String, Set<String>> merged = new TreeMap<>();
            File file = new File(INDEX_CONTENT_FILE);
            if (file.exists() && file.length() > 0) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    reader.readLine(); // Skip header
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split(",", 2);
                        if (parts.length == 2) {
                            String word = parts[0].trim();
                            Set<String> ids = new HashSet<>(Arrays.asList(parts[1].split(",")));
                            merged.put(word, ids);
                        }
                    }
                }
            }

            // Fusionar con nuevos datos
            for (Map.Entry<String, Set<String>> entry : wordToEbookNumbers.entrySet()) {
                merged.putIfAbsent(entry.getKey(), new HashSet<>());
                merged.get(entry.getKey()).addAll(entry.getValue());
            }

            // Reescribir todo el índice
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write("Word,EbookNumbers\n");
                for (Map.Entry<String, Set<String>> entry : merged.entrySet()) {
                    writer.write(entry.getKey() + "," + String.join(",", entry.getValue()) + "\n");
                }
            }

            MinioClientHelper.uploadFile(INDEX_CONTENT_FILE, "index/" + INDEX_CONTENT_FILE);
        } catch (Exception e) {
            System.err.println("Error durante escritura de contenido: " + e.getMessage());
        } finally {
            lock.unlock();
        }
    }

    private FencedLock waitForFencedLock(String name) {
        int waited = 0;
        CPSubsystem cp = Hazelcast.getHazelcastInstanceByName("hazelcast-instance").getCPSubsystem();

        while (true) {
            try {
                FencedLock lock = cp.getLock(name);
                lock.lock();
                lock.unlock();
                return cp.getLock(name);
            } catch (IllegalStateException e) {
                System.err.println("[WARN] CP subsystem no disponible aún. Esperando...");
                try {
                    Thread.sleep(RETRY_DELAY_MS);
                    waited += RETRY_DELAY_MS / 1000;
                    if (waited > LOCK_TIMEOUT_SECONDS) throw new RuntimeException("Timeout esperando al CPSubsystem.");
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrumpido esperando a FencedLock");
                }
            }
        }
    }
}
