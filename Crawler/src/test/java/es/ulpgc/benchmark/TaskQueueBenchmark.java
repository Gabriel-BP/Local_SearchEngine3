package es.ulpgc.benchmark;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.config.Config;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(org.openjdk.jmh.annotations.Scope.Thread)
public class TaskQueueBenchmark {

    private IQueue<Integer> taskQueue;

    @Setup
    public void setup() {
        Config config = new Config();
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        taskQueue = instance.getQueue("taskQueue");
    }

    @Benchmark
    public void benchmarkTaskQueueAdd() {
        taskQueue.offer(1);
    }

    @Benchmark
    public void benchmarkTaskQueuePoll() {
        taskQueue.poll();
    }
}
