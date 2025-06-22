package es.ulpgc.Indexer;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.*;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import es.ulpgc.Cleaner.Book;
import es.ulpgc.Cleaner.Cleaner;

import java.io.File;
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

        String role = System.getenv().getOrDefault("ROLE", "worker").toLowerCase();
        if (role.equals("writer")) {
            runWriter(hazelcastInstance);
        } else {
            runWorker(hazelcastInstance);
        }
    }

    private static void runWorker(HazelcastInstance hazelcastInstance) {
        IQueue<String> taskQueue = hazelcastInstance.getQueue("bookTasks");
        IMap<String, String> lastProcessedMap = hazelcastInstance.getMap("lastProcessedMap");
        IQueue<IndexResult> resultsQueue = hazelcastInstance.getQueue("resultsQueue");

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                List<String> files = listTxtOrHtmlFilesInMinio();
                files.sort(Comparator.comparing(Main::extractFilenameNumber));

                for (String path : files) {
                    String fileName = new File(path).getName();
                    if (!lastProcessedMap.containsKey(fileName) && !taskQueue.contains(path)) {
                        taskQueue.add(path);
                        System.out.println("[DEBUG] Añadido a la cola: " + path);
                    }
                }

                String filePath;
                while ((filePath = taskQueue.poll()) != null) {
                    String finalPath = filePath;
                    pool.submit(() -> {
                        String fileName = new File(finalPath).getName();
                        if (lastProcessedMap.containsKey(fileName)) return;

                        try {
                            System.out.println("[INFO] Procesando: " + fileName);
                            String localPath = "/tmp/" + fileName;
                            MinioClientHelper.downloadFile(finalPath, localPath);

                            Cleaner cleaner = new Cleaner();
                            Book book = cleaner.processBook(new File(localPath));
                            Indexer tempIndexer = new Indexer();
                            tempIndexer.buildIndexes(List.of(book));

                            IndexResult result = new IndexResult(book, tempIndexer.getBookIndexer().getHashMapIndexer().getIndex());
                            resultsQueue.add(result);
                            lastProcessedMap.put(fileName, "done");

                            System.out.println("[SUCCESS] Resultado encolado: " + book.ebookNumber);
                        } catch (Exception e) {
                            System.err.println("[ERROR] Procesando " + fileName + ": " + e.getMessage());
                        }
                    });
                }

            } catch (Exception e) {
                System.err.println("[ERROR] Worker indexTask: " + e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[INFO] Shutdown detectado. Cerrando worker...");
            scheduler.shutdownNow();
            pool.shutdownNow();
            hazelcastInstance.shutdown();
        }));
    }

    private static void runWriter(HazelcastInstance hazelcastInstance) {
        IQueue<IndexResult> queue = hazelcastInstance.getQueue("resultsQueue");
        CSVWriter writer = new CSVWriter();
        System.out.println("[WRITER] Esperando resultados...");

        final int BATCH_SIZE = 10;
        final long MAX_WAIT_MILLIS = 3000;

        List<IndexResult> batch = new ArrayList<>(BATCH_SIZE);
        long lastFlush = System.currentTimeMillis();

        while (true) {
            try {
                IndexResult result = queue.poll(500, TimeUnit.MILLISECONDS);
                long now = System.currentTimeMillis();

                if (result != null) batch.add(result);

                if (!batch.isEmpty() && (batch.size() >= BATCH_SIZE || now - lastFlush >= MAX_WAIT_MILLIS)) {
                    List<Book> books = new ArrayList<>();
                    Map<String, Set<String>> merged = new TreeMap<>();

                    batch.forEach(r -> {
                        books.add(r.book);
                        r.wordIndex.forEach((word, ids) ->
                                merged.computeIfAbsent(word, k -> new HashSet<>()).addAll(ids));
                    });

                    writer.saveMetadataToCSV(books);
                    writer.saveContentToCSV(merged);
                    System.out.println("[WRITER] Guardado lote de " + batch.size() + " libros");

                    batch.clear();
                    lastFlush = now;
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                System.err.println("[WRITER ERROR] " + e.getMessage());
            }
        }
    }

    private static List<String> listTxtOrHtmlFilesInMinio() throws Exception {
        List<String> all = MinioClientHelper.listFilesWithPrefix("raw/");
        List<String> valid = new ArrayList<>();
        for (String path : all) {
            if ((path.endsWith(".txt") || path.endsWith(".html")) && !path.endsWith("/")) {
                valid.add(path);
            }
        }
        return valid;
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
