package es.ulpgc;

import com.hazelcast.map.IMap;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class FileManager {
    private static final String DOWNLOAD_FOLDER = "app/datalake";

    // Returns the current date in "ddMMyyyy" format
    public static String getCurrentDate() {
        return LocalDate.now().format(DateTimeFormatter.ofPattern("ddMMyyyy"));
    }

    // Creates a folder at the specified path if it doesn't exist
    public static void createFolder(String folderPath) {
        try {
            Files.createDirectories(Paths.get(folderPath));
            System.out.println("Folder created: " + folderPath);
        } catch (IOException e) {
            System.err.println("Could not create folder: " + folderPath + ". Error: " + e.getMessage());
        }
    }

    // Retrieves a list of folder names within the DOWNLOAD_FOLDER directory
    public static List<String> getFoldersInPath() {
        List<String> folders = new ArrayList<>();
        File folder = new File(DOWNLOAD_FOLDER);

        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        folders.add(file.getName());
                    }
                }
            }
        }
        return folders;
    }

    // Finds the latest non-empty folder based on its date-named format
    public static String getLatestNonEmptyFolder() {
        List<String> folders = getFoldersInPath();
        String latestFolder = null;
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("ddMMyyyy");

        for (String folderName : folders) {
            try {
                LocalDate folderDate = LocalDate.parse(folderName, formatter);
                File folder = new File(DOWNLOAD_FOLDER, folderName);
                if (isFolderNonEmpty(folder) &&
                        (latestFolder == null || folderDate.isAfter(LocalDate.parse(latestFolder, formatter)))) {
                    latestFolder = folderName;
                }
            } catch (Exception e) {
                System.out.println("Skipping invalid folder format: " + folderName);
            }
        }
        return latestFolder;
    }

    // Helper method to check if a folder is non-empty
    private static boolean isFolderNonEmpty(File folder) {
        File[] files = folder.listFiles();
        return files != null && files.length > 0;
    }

    // Finds the file with the largest book ID in the specified folder
    public static String getFileWithLargestBookID(String folderName) {
        if (folderName == null) {
            System.err.println("No non-empty folder available.");
            return "010.txt";
        }

        File folder = new File(DOWNLOAD_FOLDER, folderName);
        File[] files = folder.listFiles();
        if (files == null) {
            return "010.txt";
        }

        String largestFile = null;
        int largestBookID = -1;

        for (File file : files) {
            if (file.isFile()) {
                String filename = file.getName();
                try {
                    int bookID = extractBookIDFromFilename(filename);
                    if (bookID > largestBookID) {
                        largestBookID = bookID;
                        largestFile = filename;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Skipping invalid file format: " + filename);
                }
            }
        }
        return largestFile != null ? largestFile : "010.txt";
    }

    // Extracts the book ID from a filename in the format "XX<ID>.ext"
    private static int extractBookIDFromFilename(String filename) throws NumberFormatException {
        String bookIDStr = filename.substring(2, filename.lastIndexOf('.'));
        return Integer.parseInt(bookIDStr);
    }

    // Saves the Hazelcast IMap progressMap to a file
    public static void saveProgressMap(IMap<Integer, Boolean> progressMap, String filePath) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filePath))) {
            Map<Integer, Boolean> localMap = new HashMap<>(progressMap);
            oos.writeObject(localMap);
            System.out.println("Progress map saved successfully to " + filePath);
        } catch (IOException e) {
            System.err.println("Failed to save progress map to " + filePath + ": " + e.getMessage());
        }
    }

    // Loads the progressMap from a file into a Hazelcast IMap
    @SuppressWarnings("unchecked")
    public static void loadProgressMap(IMap<Integer, Boolean> progressMap, String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            System.out.println("Progress map file not found at " + filePath + ". Starting with an empty map.");
            return;
        }

        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
            Map<Integer, Boolean> localMap = (Map<Integer, Boolean>) ois.readObject();
            progressMap.putAll(localMap);
            System.out.println("Progress map loaded successfully from " + filePath);
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to load progress map from " + filePath + ": " + e.getMessage());
        }
    }
}
