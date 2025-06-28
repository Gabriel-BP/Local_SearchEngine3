package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class DataMartWriter {

    private static final String CONTENT_DATAMART_DIR = "/app/datamart_content";
    private static final String METADATA_DATAMART_DIR = "/app/datamart_metadata";

    public void saveContentToDataMart(Map<String, Set<String>> wordToEbookNumbers) {
        File rootDir = new File(CONTENT_DATAMART_DIR);

        if (!rootDir.exists() && !rootDir.mkdirs()) {
            System.err.println("Failed to create content data mart directory.");
            return;
        }

        for (Map.Entry<String, Set<String>> entry : wordToEbookNumbers.entrySet()) {
            String word = entry.getKey();
            Set<String> ebookNumbers = entry.getValue();

            try {
                createTrieStructure(rootDir, word, ebookNumbers);
            } catch (IOException e) {
                System.err.println("Error creating structure for word '" + word + "': " + e.getMessage());
            }
        }

        System.out.println("Content index saved to datamart_content in " + CONTENT_DATAMART_DIR);
    }

    private void createTrieStructure(File currentDir, String word, Set<String> ebookNumbers) throws IOException {
        for (int i = 0; i < word.length(); i++) {
            String subDirName = sanitizeName(word.substring(0, i + 1));
            File nextDir = new File(currentDir, subDirName);

            if (!nextDir.exists() && !nextDir.mkdirs()) {
                throw new IOException("Failed to create directory: " + nextDir.getPath());
            }

            currentDir = nextDir;

            if (i == word.length() - 1) {
                String fileName = sanitizeName(word) + ".txt";
                File wordFile = new File(currentDir, fileName);

                if (wordFile.exists()) {
                    updateReferencesInFile(wordFile, ebookNumbers);
                } else {
                    try (FileWriter writer = new FileWriter(wordFile)) {
                        writer.write("{\"word\": \"" + word + "\", \"references\": [");
                        writer.write(ebookNumbers.stream()
                                .map(ref -> "\"" + ref + "\"")
                                .collect(Collectors.joining(",")));
                        writer.write("]}");
                    }
                }

                // ✅ Nuevo: subir a MinIO después de guardar local
                String relativePath = wordFile.getAbsolutePath().replace("/app/", "");
                try {
                    MinioClientHelper.uploadFile(wordFile.getAbsolutePath(), relativePath);
                } catch (Exception e) {
                    System.err.println("[UPLOAD ERROR] Failed to upload: " + relativePath + " -> " + e.getMessage());
                }
            }
        }
    }

    private void updateReferencesInFile(File wordFile, Set<String> ebookNumbers) throws IOException {
        String content = new String(Files.readAllBytes(wordFile.toPath()));

        int referencesStartIndex = content.indexOf("\"references\": [") + 15;
        int referencesEndIndex = content.indexOf("]", referencesStartIndex);
        String existingReferencesStr = content.substring(referencesStartIndex, referencesEndIndex);
        Set<String> existingReferences = new HashSet<>(Arrays.asList(existingReferencesStr.replace("\"", "").split(",")));

        existingReferences.addAll(ebookNumbers);

        String updatedContent = content.substring(0, referencesStartIndex) +
                existingReferences.stream()
                        .map(ref -> "\"" + ref + "\"")
                        .collect(Collectors.joining(",")) +
                content.substring(referencesEndIndex);

        Files.write(wordFile.toPath(), updatedContent.getBytes());

        // ✅ Subir después de actualizar
        String relativePath = wordFile.getAbsolutePath().replace("/app/", "");
        try {
            MinioClientHelper.uploadFile(wordFile.getAbsolutePath(), relativePath);
        } catch (Exception e) {
            System.err.println("[UPLOAD ERROR] Failed to upload updated: " + relativePath + " -> " + e.getMessage());
        }
    }

    private String sanitizeName(String name) {
        String[] reservedNames = {
                "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4",
                "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2",
                "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"
        };
        for (String reserved : reservedNames) {
            if (name.equalsIgnoreCase(reserved)) {
                return "_" + name;
            }
        }
        return name.replaceAll("[<>:\"/\\\\|?*]", "_");
    }

    public void saveMetadataToDataMart(Iterable<Book> books) {
        File rootDir = new File(METADATA_DATAMART_DIR);

        if (!rootDir.exists() && !rootDir.mkdirs()) {
            System.err.println("Failed to create metadata data mart directory.");
            return;
        }

        for (Book book : books) {
            File bookDir = new File(rootDir, book.ebookNumber);

            if (!bookDir.exists() && !bookDir.mkdirs()) {
                System.err.println("Failed to create directory for ebook " + book.ebookNumber);
                continue;
            }

            File metadataFile = new File(bookDir, "metadata.json");

            try (FileWriter writer = new FileWriter(metadataFile)) {
                writer.write("{\n");
                writer.write("  \"Title\": \"" + book.title + "\",\n");
                writer.write("  \"Author\": \"" + book.author + "\",\n");
                writer.write("  \"Date\": \"" + book.date + "\",\n");
                writer.write("  \"Language\": \"" + book.language + "\",\n");
                writer.write("  \"Credits\": \"" + book.credits + "\"\n");
                writer.write("}");
            } catch (IOException e) {
                System.err.println("Error writing metadata for ebook " + book.ebookNumber + ": " + e.getMessage());
            }

            // ✅ Subir metadata
            String relativePath = metadataFile.getAbsolutePath().replace("/app/", "");
            try {
                MinioClientHelper.uploadFile(metadataFile.getAbsolutePath(), relativePath);
            } catch (Exception e) {
                System.err.println("[UPLOAD ERROR] Failed to upload metadata: " + relativePath + " -> " + e.getMessage());
            }
        }

        System.out.println("Metadata saved to datamart_metadata in " + METADATA_DATAMART_DIR);
    }
}
