package es.ulpgc.Cleaner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LastProcessedTracker {
    private final Path lastProcessedFilePath;

    public LastProcessedTracker(String filePath) {
        this.lastProcessedFilePath = Path.of(filePath);
    }

    public String getLastProcessed() {
        try {
            if (Files.exists(lastProcessedFilePath)) {
                return Files.readString(lastProcessedFilePath).trim();
            }
        } catch (IOException e) {
            System.err.println("Error reading LastProcessed.txt: " + e.getMessage());
        }
        return null;
    }

    public void updateLastProcessed(String fileName) {
        try {
            String baseName = fileName.replaceFirst("[.][^.]+$", ""); // Remove the extension
            Files.writeString(lastProcessedFilePath, baseName, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            System.err.println("Error writing to LastProcessed.txt: " + e.getMessage());
        }
    }
}

