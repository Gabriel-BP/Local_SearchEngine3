package es.ulpgc;

import com.hazelcast.collection.IQueue;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.map.IMap;

import java.net.*;
import java.util.*;

public class Main {
    private static final String PROGRESS_MAP_FILE = "progressMap.dat";

    public static void main(String[] args) {
        Config config = new Config();
        config.setClusterName("search-cluster");

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(5701).setPortAutoIncrement(true);

        // Detectar IP válida del contenedor para setPublicAddress()
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
