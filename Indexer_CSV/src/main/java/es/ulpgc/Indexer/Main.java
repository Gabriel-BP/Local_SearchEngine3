package es.ulpgc.Indexer;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import es.ulpgc.Cleaner.Book;
import es.ulpgc.Cleaner.Cleaner;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {
        Config config = new Config();
        config.setClusterName("indexer-cluster");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(5701).setPortAutoIncrement(true);

        JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);

        joinConfig.getTcpIpConfig()
                .setEnabled(true)
                .addMember("192.168.56.1")
                .addMember("192.168.0.237");

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        IQueue<String> taskQueue = hazelcastInstance.getQueue("bookTasks");
        IMap<String, String> lastProcessedMap = hazelcastInstance.getMap("lastProcessedMap");

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        Runnable task = () -> {
            System.out.println("[INFO] Indexer scheduled every 30 seconds.");

            String lastProcessedFile = lastProcessedMap.get("lastProcessed");
            System.out.println("[INFO] Last processed file: " + lastProcessedFile);

            boolean isMaster = hazelcastInstance.getCluster().getLocalMember().equals(
                    hazelcastInstance.getCluster().getMembers().iterator().next()
            );
            System.out.println("[DEBUG] Members: " + hazelcastInstance.getCluster().getMembers());
            System.out.println("[DEBUG] Local: " + hazelcastInstance.getCluster().getLocalMember());

            if (taskQueue.isEmpty() && isMaster) {
                System.out.println("[INFO] Task queue is empty. Attempting to initialize...");
                try {
                    List<String> files = listTxtOrHtmlFilesInMinio();
                    files.sort(Comparator.comparing(Main::extractFilenameNumber));
                    boolean addFiles = lastProcessedFile == null;
                    for (String path : files) {
                        if (!addFiles && path.endsWith("/" + lastProcessedFile)) {
                            addFiles = true;
                            continue;
                        }
                        if (addFiles) {
                            System.out.println("[DEBUG] Adding to queue: " + path);
                            taskQueue.add(path);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("[ERROR] Failed to initialize task queue: " + e.getMessage());
                }
                System.out.println("[INFO] Task queue initialized.");
            }

            String filePath;
            try {
                while ((filePath = taskQueue.poll()) != null) {
                    System.out.println("[INFO] Processing file: " + filePath);
                    String localPath = "/tmp/" + new File(filePath).getName();
                    try {
                        MinioClientHelper.downloadFile(filePath, localPath);
                    } catch (Exception e) {
                        System.err.println("[ERROR] Failed to download from MinIO: " + e.getMessage());
                        continue;
                    }

                    Cleaner cleaner = new Cleaner();
                    Book book = cleaner.processBook(new File(localPath));
                    Indexer indexer = new Indexer();
                    indexer.indexBooks(Collections.singletonList(book), "csv");
                    System.out.println("[INFO] File processed and indexed: " + filePath);
                    lastProcessedMap.put("lastProcessed", new File(filePath).getName());
                }
                System.out.println("[INFO] No more tasks in queue.");
            } catch (IOException e) {
                System.err.println("[ERROR] Processing failure: " + e.getMessage());
            }
        };

        scheduler.scheduleAtFixedRate(task, 0, 30, TimeUnit.SECONDS);
    }

    private static List<String> listTxtOrHtmlFilesInMinio() throws Exception {
        List<String> allFiles = MinioClientHelper.listFilesWithPrefix("raw/");
        System.out.println("[DEBUG] Todos los objetos encontrados en MinIO:");
        for (String file : allFiles) {
            System.out.println("  - " + file);
        }

        List<String> validFiles = new ArrayList<>();
        for (String path : allFiles) {
            if ((path.endsWith(".txt") || path.endsWith(".html")) && !path.endsWith("/")) {
                validFiles.add(path);
            }
        }

        System.out.println("[INFO] Valid .txt/.html files found in MinIO: " + validFiles.size());
        return validFiles;
    }

    private static int extractFilenameNumber(String path) {
        try {
            String name = new File(path).getName().replaceFirst("[.][^.]+$", "");
            return Integer.parseInt(name.replaceAll("\\D", ""));
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
    }
}
