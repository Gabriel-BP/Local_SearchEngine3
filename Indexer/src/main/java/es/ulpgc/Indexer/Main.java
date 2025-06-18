package es.ulpgc.Indexer;

import com.hazelcast.collection.IQueue;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import es.ulpgc.Cleaner.Book;
import es.ulpgc.Cleaner.Cleaner;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        // Start a Hazelcast instance
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();

        // Define the distributed queue for task coordination
        IQueue<String> taskQueue = hazelcastInstance.getQueue("bookTasks");

        // Define the distributed map to track the last processed file
        IMap<String, String> lastProcessedMap = hazelcastInstance.getMap("lastProcessedMap");

        // Define the scheduled executor service to run the task periodically
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Task to initialize the queue and process files every 30 seconds
        Runnable task = () -> {
            // Get the last processed file from the distributed map
            String lastProcessedFile = lastProcessedMap.get("lastProcessed");
            System.out.println("Last processed file: " + lastProcessedFile);

            // Initialize the queue with tasks if it's empty (only one member does this)
            if (hazelcastInstance.getCluster().getMembers().iterator().next().localMember()) {
                System.out.println("Initializing task queue...");

                // Traverse all subdirectories of 'datalake' and add files to the task queue
                File rootFolder = new File("/app/datalake");
                addFilesToQueue(rootFolder, taskQueue, lastProcessedFile);

                System.out.println("Task queue initialized.");
            }

            // Continuously process tasks from the queue
            String filePath = null;
            try {
                while ((filePath = taskQueue.poll()) != null) {
                    System.out.println("Processing file: " + filePath);
                    File file = new File(filePath);
                    Cleaner cleaner = new Cleaner();
                    Book book = cleaner.processBook(file); // Process the book
                    Indexer indexer = new Indexer();
                    indexer.indexBooks(Collections.singletonList(book), "datamart"); // Index the processed book
                    System.out.println("File processed and indexed successfully: " + filePath);

                    // Update the last processed file in the distributed map
                    lastProcessedMap.put("lastProcessed", file.getName()); // Track the last processed file
                }
                System.out.println("No more tasks available in the queue.");
            } catch (IOException e) {
                System.err.println("Error processing file: " + filePath + ". " + e.getMessage());
            }
        };

        // Schedule the task to run every 30 seconds
        scheduler.scheduleAtFixedRate(task, 0, 30, TimeUnit.SECONDS);

        System.out.println("Task scheduled to run every 30 seconds.");
    }

    private static void addFilesToQueue(File folder, IQueue<String> taskQueue, String lastProcessedFile) {
        if (folder.exists() && folder.isDirectory()) {
            // Get all .txt and .html files in the folder
            File[] files = folder.listFiles((dir, name) -> name.endsWith(".txt") || name.endsWith(".html"));
            if (files != null) {
                // Sort files numerically by extracting the numeric part of the file name
                Arrays.sort(files, new Comparator<File>() {
                    @Override
                    public int compare(File f1, File f2) {
                        // Extract numeric parts of file names
                        int num1 = extractNumberFromFile(f1);
                        int num2 = extractNumberFromFile(f2);
                        return Integer.compare(num1, num2);
                    }
                });

                boolean addFiles = false;

                // If lastProcessedFile is null, start adding files from the beginning
                if (lastProcessedFile == null) {
                    addFiles = true;
                }

                // Add files to the task queue starting from the file after the last processed one
                for (File file : files) {
                    // If we haven't found the last processed file yet, continue
                    if (!addFiles) {
                        if (file.getName().equals(lastProcessedFile)) {
                            addFiles = true; // Start adding files after this one
                        }
                        continue;
                    }

                    // Add the file to the task queue
                    taskQueue.add(file.getAbsolutePath());
                }
            }

            // Recursively add files from subdirectories
            File[] subfolders = folder.listFiles(File::isDirectory);
            if (subfolders != null) {
                for (File subfolder : subfolders) {
                    addFilesToQueue(subfolder, taskQueue, lastProcessedFile); // Recurse into subdirectories
                }
            }
        }
    }

    // Helper method to extract the numeric part from the file name
    private static int extractNumberFromFile(File file) {
        String fileName = file.getName().replaceFirst("[.][^.]+$", ""); // Remove file extension
        try {
            // Assuming the numeric part starts after the first two characters (e.g., 011.txt -> 11)
            return Integer.parseInt(fileName.substring(2)); // Extract and return the number
        } catch (NumberFormatException e) {
            System.err.println("Error parsing number from file name: " + file.getName());
            return Integer.MAX_VALUE; // Default to a large number if parsing fails
        }
    }
}
