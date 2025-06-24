package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.io.*;
import java.util.*;

public class CSVWriter {
    private static final String INDEX_METADATA_FILE = "index_metadata.csv";
    private static final String INDEX_CONTENT_FILE = "index_content.csv";

    public void saveMetadataToCSV(Iterable<Book> books) {
        try {
            boolean descargado = false;
            try {
                MinioClientHelper.downloadFile("index/" + INDEX_METADATA_FILE, INDEX_METADATA_FILE);
                descargado = true;
            } catch (Exception e) {
                File local = new File(INDEX_METADATA_FILE);
                if (local.exists()) {
                    descargado = true;
                    System.out.println("[INFO] Archivo local " + INDEX_METADATA_FILE + " ya presente, se usará como base.");
                } else {
                    System.err.println("[WARN] No se pudo descargar " + INDEX_METADATA_FILE + ", se asumirá vacío.");
                }
            }

            File file = new File(INDEX_METADATA_FILE);
            if (!descargado && !file.exists()) {
                if (!file.createNewFile()) {
                    System.err.println("[WARN] No se pudo crear el archivo local " + file.getName());
                }
            }

            Set<String> existing = new HashSet<>();
            List<String> allLines = readMetadataLines(file, existing);

            for (Book book : books) {
                String id = book.ebookNumber.trim().toLowerCase();
                if (!existing.contains(id)) {
                    String row = String.format("%s,\"%s\",\"%s\",\"%s\",\"%s\",\"%s\"",
                            escape(book.ebookNumber), escape(book.title), escape(book.author),
                            escape(book.date), escape(book.language), escape(book.credits));
                    allLines.add(row);
                    existing.add(id);
                }
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                for (String line : allLines) writer.write(line + "\n");
            }

            MinioClientHelper.uploadFile(INDEX_METADATA_FILE, "index/" + INDEX_METADATA_FILE);
        } catch (Exception e) {
            System.err.println("Error durante escritura de metadatos: " + e.getMessage());
        }
    }

    public void saveContentToCSV(Map<String, Set<String>> wordToEbookNumbers) {
        try {
            boolean descargado = false;
            try {
                MinioClientHelper.downloadFile("index/" + INDEX_CONTENT_FILE, INDEX_CONTENT_FILE);
                descargado = true;
            } catch (Exception e) {
                File local = new File(INDEX_CONTENT_FILE);
                if (local.exists()) {
                    descargado = true;
                    System.out.println("[INFO] Archivo local " + INDEX_CONTENT_FILE + " ya presente, se usará como base.");
                } else {
                    System.err.println("[WARN] No se pudo descargar " + INDEX_CONTENT_FILE + ", se asumirá vacío.");
                }
            }

            File file = new File(INDEX_CONTENT_FILE);
            if (!descargado && !file.exists()) {
                if (!file.createNewFile()) {
                    System.err.println("[WARN] No se pudo crear el archivo local " + file.getName());
                }
            }

            Map<String, Set<String>> merged = readContentIndex(file);

            for (Map.Entry<String, Set<String>> entry : wordToEbookNumbers.entrySet()) {
                merged.putIfAbsent(entry.getKey(), new HashSet<>());
                merged.get(entry.getKey()).addAll(entry.getValue());
            }

            try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
                writer.write("Word,EbookNumbers\n");
                for (Map.Entry<String, Set<String>> entry : merged.entrySet()) {
                    writer.write(entry.getKey() + "," + String.join(",", entry.getValue()) + "\n");
                }
            }

            MinioClientHelper.uploadFile(INDEX_CONTENT_FILE, "index/" + INDEX_CONTENT_FILE);
        } catch (Exception e) {
            System.err.println("Error durante escritura de contenido: " + e.getMessage());
        }
    }

    private List<String> readMetadataLines(File file, Set<String> existing) throws IOException {
        List<String> allLines = new ArrayList<>();
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
        return allLines;
    }

    private Map<String, Set<String>> readContentIndex(File file) throws IOException {
        Map<String, Set<String>> merged = new TreeMap<>();
        if (file.exists() && file.length() > 0) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                reader.readLine(); // Saltar cabecera
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
        return merged;
    }

    private String escape(String input) {
        return input == null ? "" : input.replace("\"", "\"\"");
    }
}
