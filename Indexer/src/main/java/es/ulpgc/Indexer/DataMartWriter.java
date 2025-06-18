package es.ulpgc.Indexer;

import es.ulpgc.Cleaner.Book;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DataMartWriter {
    private static final String CONTENT_DATAMART_DIR = "/app/datamart_content";
    private static final String METADATA_DATAMART_DIR = "/app/datamart_metadata";

    /**
     * Save content index to datamart_content following a trie-like directory structure.
     */
    public void saveContentToDataMart(Map<String, Set<String>> wordToEbookNumbers) {
        File rootDir = new File(CONTENT_DATAMART_DIR);

        // Create root directory if it doesn't exist
        if (!rootDir.exists() && !rootDir.mkdirs()) {
            System.err.println("Failed to create content data mart directory.");
            return;
        }

        // Process each word and create the trie structure
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
            String subDirName = word.substring(0, i + 1); // Prefix of the word up to the current character
            subDirName = sanitizeName(subDirName); // Sanitize folder name to avoid conflicts
            File nextDir = new File(currentDir, subDirName);

            // Intermediate directory creation
            if (!nextDir.exists() && !nextDir.mkdirs()) {
                throw new IOException("Failed to create directory: " + nextDir.getPath());
            }

            currentDir = nextDir; // Move to the next directory in the trie

            if (i == word.length() - 1) {
                // At the last character, create or update the .txt file with the word's name
                String fileName = sanitizeName(word) + ".txt";
                File wordFile = new File(currentDir, fileName);

                if (wordFile.exists()) {
                    // Word already exists, update the references
                    updateReferencesInFile(wordFile, ebookNumbers);
                } else {
                    // Word is new, create the file
                    try (FileWriter writer = new FileWriter(wordFile)) {
                        writer.write("{\"word\": \"" + word + "\", \"references\": [");
                        writer.write(ebookNumbers.stream()
                                .map(ref -> "\"" + ref + "\"") // Enclose each reference in quotes
                                .collect(Collectors.joining(",")));
                        writer.write("]}");
                    }
                }
            }
        }
    }



    /**
     * Updates the references in the existing word file by appending new ebook numbers.
     */
    private void updateReferencesInFile(File wordFile, Set<String> ebookNumbers) throws IOException {
        // Read existing data from the file
        String content = new String(Files.readAllBytes(wordFile.toPath()));

        // Extract the current references (assuming JSON format)
        int referencesStartIndex = content.indexOf("\"references\": [") + 15;
        int referencesEndIndex = content.indexOf("]", referencesStartIndex);
        String existingReferencesStr = content.substring(referencesStartIndex, referencesEndIndex);
        Set<String> existingReferences = new HashSet<>(Arrays.asList(existingReferencesStr.replace("\"", "").split(",")));

        // Append the new ebook numbers to the existing references
        existingReferences.addAll(ebookNumbers);

        // Write back the updated content
        String updatedContent = content.substring(0, referencesStartIndex) +
                existingReferences.stream()
                        .map(ref -> "\"" + ref + "\"")
                        .collect(Collectors.joining(",")) +
                content.substring(referencesEndIndex);

        Files.write(wordFile.toPath(), updatedContent.getBytes());
    }


    /**
     * Sanitizes folder or file names to avoid using reserved names in Windows or invalid characters.
     */
    private String sanitizeName(String name) {
        // List of reserved names in Windows
        String[] reservedNames = {
                "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5",
                "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5",
                "LPT6", "LPT7", "LPT8", "LPT9"
        };

        // Replace reserved names with a safe equivalent
        for (String reserved : reservedNames) {
            if (name.equalsIgnoreCase(reserved)) {
                return "_" + name; // Prefix with an underscore to make it valid
            }
        }

        // Replace invalid characters in file and folder names
        return name.replaceAll("[<>:\"/\\\\|?*]", "_");
    }


    /**
     * Save metadata for books into datamart_metadata, with each ebook having its own folder.
     */
    public void saveMetadataToDataMart(Iterable<Book> books) {
        File rootDir = new File(METADATA_DATAMART_DIR);

        // Create root directory if it doesn't exist
        if (!rootDir.exists() && !rootDir.mkdirs()) {
            System.err.println("Failed to create metadata data mart directory.");
            return;
        }

        for (Book book : books) {
            File bookDir = new File(rootDir, book.ebookNumber);

            // Create a folder for the ebook if it doesn't already exist
            if (!bookDir.exists() && !bookDir.mkdirs()) {
                System.err.println("Failed to create directory for ebook " + book.ebookNumber);
                continue;
            }

            File metadataFile = new File(bookDir, "metadata.json");

            // Write metadata to the file
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
        }

        System.out.println("Metadata saved to datamart_metadata in " + METADATA_DATAMART_DIR);
    }
}
