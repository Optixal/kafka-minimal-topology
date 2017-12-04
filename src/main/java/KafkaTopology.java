import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.EARLIEST;

public class KafkaTopology {

    private static final String TOPOLOGY_NAME = "KafkaMinimal";
    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    private static final String TOPIC_0 = "test";
    private static final String TOPIC_0_1_STREAM = "test_0_1_stream";

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        config.setDebug(true);

        // Remote - Command Line storm
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, getTopologyKafkaSpout(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER)));
        // Local
        } else {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, getTopologyKafkaSpout(getKafkaSpoutConfig(KAFKA_LOCAL_BROKER)));
            Thread.sleep(10000);
            cluster.shutdown();
        }

    }

    protected static StormTopology getTopologyKafkaSpout(KafkaSpoutConfig<String, String> spoutConfig) {
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", new KafkaSpout<>(spoutConfig), 1);
        tp.setBolt("kafka_bolt", new KafkaTestBolt())
                .shuffleGrouping("kafka_spout", TOPIC_0_1_STREAM);
        return tp.createTopology();
    }

    protected static KafkaSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers) {
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
            (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
            new Fields("topic", "partition", "offset", "key", "value"), TOPIC_0_1_STREAM);
        return KafkaSpoutConfig.builder(bootstrapServers, new String[]{TOPIC_0})
            .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
            .setRetry(getRetryService())
            .setRecordTranslator(trans)
            .setOffsetCommitPeriodMs(10_000)
            .setFirstPollOffsetStrategy(EARLIEST)
            .setMaxUncommittedOffsets(250)
            .build();
    }

    protected static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
            TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }
}
