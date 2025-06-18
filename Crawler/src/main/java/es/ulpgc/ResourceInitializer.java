package es.ulpgc;

import com.hazelcast.collection.IQueue;
import com.hazelcast.map.IMap;

public class ResourceInitializer {
    private final IQueue<Integer> taskQueue;
    private final IMap<Integer, Boolean> progressMap;
    private final String progressMapFile;

    public ResourceInitializer(IQueue<Integer> taskQueue, IMap<Integer, Boolean> progressMap, String progressMapFile) {
        this.taskQueue = taskQueue;
        this.progressMap = progressMap;
        this.progressMapFile = progressMapFile;
    }

    public void initialize() {
        FileManager.loadProgressMap(progressMap, progressMapFile);

        if (taskQueue.isEmpty()) {
            System.out.println("Initializing task queue...");
            populateTaskQueue();
        }
    }

    private void populateTaskQueue() {
        int totalBooks = 10000;
        for (int i = 1; i <= totalBooks; i++) {
            if (!progressMap.getOrDefault(i, false)) {
                boolean offered = taskQueue.offer(i);
                if (!offered) {
                    System.err.println("Failed to add book ID #" + i + " to the queue.");
                }
            }
        }
    }
}
