package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class CSVWriter {
    private static final String INDEX_METADATA_FILE = "index_metadata.csv";
    private static final String INDEX_CONTENT_FILE = "index_content.csv";

    public void saveMetadataToCSV(Iterable<Book> books) {
        File file = new File(INDEX_METADATA_FILE);
        Set<String> existingEbookNumbers = Collections.newSetFromMap(new ConcurrentHashMap<>());

        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                reader.readLine(); // Saltar cabecera
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",", 2);
                    if (parts.length > 0) {
                        existingEbookNumbers.add(parts[0].trim().toLowerCase());
                    }
                }
            } catch (IOException e) {
                System.err.println("Error leyendo metadatos: " + e.getMessage());
            }
        }

        try (BufferedWriter metadataWriter = new BufferedWriter(new FileWriter(file, true))) {
            for (Book book : books) {
                String normalizedEbookNumber = book.ebookNumber.trim().toLowerCase();
                if (!existingEbookNumbers.contains(normalizedEbookNumber)) {
                    synchronized (metadataWriter) {
                        metadataWriter.write(String.format("%s,%s,%s,%s,%s,%s%n",
                                book.ebookNumber, book.title, book.author, book.date, book.language, book.credits));
                    }
                    existingEbookNumbers.add(normalizedEbookNumber);
                }
            }
        } catch (IOException e) {
            System.err.println("Error escribiendo metadatos en CSV: " + e.getMessage());
        }

        try {
            MinioClientHelper.uploadFile(INDEX_METADATA_FILE, "index/" + INDEX_METADATA_FILE);
        } catch (Exception e) {
            System.err.println("Error subiendo " + INDEX_METADATA_FILE + " a MinIO: " + e.getMessage());
        }
    }

    public void saveContentToCSV(Map<String, Set<String>> wordToEbookNumbers) {
        File file = new File(INDEX_CONTENT_FILE);
        Set<String> existingLines = new HashSet<>();

        if (file.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                reader.readLine(); // Saltar cabecera
                String line;
                while ((line = reader.readLine()) != null) {
                    existingLines.add(line.trim());
                }
            } catch (IOException e) {
                System.err.println("Error leyendo índice previo: " + e.getMessage());
            }
        }

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            if (existingLines.isEmpty()) {
                writer.write("Word,EbookNumbers\n");
            }

            for (Map.Entry<String, Set<String>> entry : wordToEbookNumbers.entrySet()) {
                String line = entry.getKey() + "," + String.join(",", entry.getValue());
                if (!existingLines.contains(line)) {
                    writer.write(line + "\n");
                }
            }
        } catch (IOException e) {
            System.err.println("Error escribiendo índice en CSV: " + e.getMessage());
        }

        try {
            MinioClientHelper.uploadFile(INDEX_CONTENT_FILE, "index/" + INDEX_CONTENT_FILE);
        } catch (Exception e) {
            System.err.println("Error subiendo " + INDEX_CONTENT_FILE + " a MinIO: " + e.getMessage());
        }
    }
}
