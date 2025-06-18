package es.ulpgc;

import com.hazelcast.collection.IQueue;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;

public class Main {
    private static final String PROGRESS_MAP_FILE = "progressMap.dat";

    public static void main(String[] args) {
        // ==== CONFIGURACIÓN DISTRIBUIDA DE HAZELCAST ====
        Config config = new Config();
        config.setClusterName("crawler-cluster");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(5701).setPortAutoIncrement(true);

        JoinConfig join = network.getJoin();
        join.getMulticastConfig().setEnabled(false);

        join.getTcpIpConfig()
                .setEnabled(true)
                .addMember("192.168.0.56")
                .addMember("192.168.0.237");

        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);

        System.out.println("Miembros en el clúster:");
        for (Member member : hazelcast.getCluster().getMembers()) {
            System.out.println(" - " + member.getAddress());
        }

        IQueue<Integer> taskQueue = hazelcast.getQueue("bookIdQueue");
        IMap<Integer, Boolean> progressMap = hazelcast.getMap("progressMap");

        ResourceInitializer resourceInitializer = new ResourceInitializer(taskQueue, progressMap, PROGRESS_MAP_FILE);
        resourceInitializer.initialize();

        CrawlerManager crawlerManager = new CrawlerManager(taskQueue, progressMap, PROGRESS_MAP_FILE);
        crawlerManager.startCrawling();

        hazelcast.shutdown();
    }
}
