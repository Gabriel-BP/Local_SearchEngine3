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
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Main {
    public static void main(String[] args) {
        Config config = new Config();
        config.setClusterName("search-cluster");
        config.setInstanceName("hazelcast-instance");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(5701).setPortAutoIncrement(true);

        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface ni = interfaces.nextElement();
                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress addr = addresses.nextElement();
                    if (!addr.isLoopbackAddress() && addr instanceof Inet4Address) {
                        network.setPublicAddress(addr.getHostAddress());
                        System.out.println("[INFO] IP detectada para Hazelcast: " + addr.getHostAddress());
                        break;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("[ERROR] No se pudo detectar IP local del contenedor: " + e.getMessage());
        }

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);

        String hazelcastMembers = System.getenv().getOrDefault("HZ_MEMBERS", "host.docker.internal");
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig().setEnabled(true);
        for (String member : hazelcastMembers.split(",")) {
            System.out.println("[INFO] Añadiendo miembro Hazelcast: " + member.trim());
            tcpIpConfig.addMember(member.trim());
        }

        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        IQueue<String> taskQueue = hazelcastInstance.getQueue("bookTasks");
        IMap<String, String> lastProcessedMap = hazelcastInstance.getMap("lastProcessedMap");

        Indexer indexer = new Indexer(); // Se crea una vez
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

        Runnable task = () -> {
            System.out.println("[INFO] Indexer scheduled every 30 seconds.");

            try {
                List<String> files = listTxtOrHtmlFilesInMinio();
                files.sort(Comparator.comparing(Main::extractFilenameNumber));

                for (String path : files) {
                    String fileName = new File(path).getName();
                    if (!lastProcessedMap.containsKey(fileName) && !taskQueue.contains(path)) {
                        System.out.println("[DEBUG] Añadiendo nuevo archivo a la cola: " + path);
                        taskQueue.add(path);
                    }
                }
                System.out.println("[INFO] Cola actualizada con nuevos archivos (si los había)");
            } catch (Exception e) {
                System.err.println("[ERROR] Fallo al actualizar la cola desde MinIO: " + e.getMessage());
            }

            try {
                String filePath;
                while ((filePath = taskQueue.poll()) != null) {
                    String fileName = new File(filePath).getName();
                    if (lastProcessedMap.containsKey(fileName)) {
                        System.out.println("[SKIP] Ya procesado: " + fileName);
                        continue;
                    }

                    System.out.println("[INFO] Processing file: " + filePath);
                    String localPath = "/tmp/" + fileName;
                    try {
                        MinioClientHelper.downloadFile(filePath, localPath);
                    } catch (Exception e) {
                        System.err.println("[ERROR] Failed to download from MinIO: " + e.getMessage());
                        continue;
                    }

                    Cleaner cleaner = new Cleaner();
                    Book book = cleaner.processBook(new File(localPath));
                    indexer.indexBooks(Collections.singletonList(book), "csv");

                    System.out.println("[SUCCESS] Libro indexado correctamente y subido a MinIO: " + book.ebookNumber);
                    lastProcessedMap.put(fileName, "done");
                }

                System.out.println("[INFO] No more tasks in queue.");
            } catch (IOException e) {
                System.err.println("[ERROR] Processing failure: " + e.getMessage());
            }
        };

        scheduler.scheduleAtFixedRate(task, 0, 30, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[INFO] Shutdown detected. Liberando recursos...");
            scheduler.shutdownNow();
            indexer.shutdown();
            hazelcastInstance.shutdown();
        }));
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
