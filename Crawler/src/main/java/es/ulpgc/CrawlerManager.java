package es.ulpgc;

import com.hazelcast.collection.IQueue;
import com.hazelcast.map.IMap;

public class CrawlerManager {
    private final IQueue<Integer> taskQueue;
    private final IMap<Integer, Boolean> progressMap;
    private final String progressMapFile;
    private final GutenbergCrawler crawler;

    public CrawlerManager(IQueue<Integer> taskQueue, IMap<Integer, Boolean> progressMap, String progressMapFile) {
        this.taskQueue = taskQueue;
        this.progressMap = progressMap;
        this.progressMapFile = progressMapFile;
        this.crawler = new GutenbergCrawler();
    }

    public void startCrawling() {
        System.out.println("Starting crawling process...");

        while (true) {
            Integer bookId = taskQueue.poll();
            if (bookId == null) {
                System.out.println("No more tasks available. Exiting...");
                break;
            }

            processTask(bookId);
            // wait for 5 seconds before processing the next task
            try {
                Thread.sleep(2500);
            } catch (InterruptedException e) {
                System.err.println("Error while waiting for the next task: " + e.getMessage());
            }
        }

        // Save progress map before exiting
        FileManager.saveProgressMap(progressMap, progressMapFile);
        System.out.println("Crawling process completed.");
    }

    private void processTask(Integer bookId) {
        try {
            if (progressMap.getOrDefault(bookId, false)) {
                System.out.println("Book ID #" + bookId + " already processed. Skipping...");
                return;
            }

            System.out.println("Crawling book ID #" + bookId);
            crawler.crawlBooks(bookId, 1); // Process one book
            progressMap.put(bookId, true);
            FileManager.saveProgressMap(progressMap, progressMapFile);
            System.out.println("Book ID #" + bookId + " crawled successfully.");
        } catch (Exception e) {
            System.err.println("Error during crawling book ID #" + bookId + ": " + e.getMessage());
        }
    }
}
