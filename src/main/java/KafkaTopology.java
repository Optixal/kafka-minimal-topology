import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Properties;

import static org.apache.storm.kafka.spout.KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST;

public class KafkaTopology {

    private static final String TOPOLOGY_NAME = "KafkaMinimal";
    private static final String STREAM_INPUT_TO_FIRST_BOLT = "stream";
    private static final String FINAL_OUTPUT_FIELD_NAME = "output";

    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";
    private static final String KAFKA_GROUP = "kafkaSpoutTestGroup";
    private static final String KAFKA_TOPIC_INPUT = "test";
    private static final String KAFKA_TOPIC_OUTPUT = "test_out";

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);

        // Remote - Command Line storm
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], config, getTopology());
        // Local
        } else {
            config.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            StormSubmitter.submitTopology(TOPOLOGY_NAME, config, getTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }

    protected static StormTopology getTopology() {
        // Topology: KafkaSpout ("value") -> KafkaTestBolt ("length") -> KafkaTestBolt2 ("output")
        final TopologyBuilder tp = new TopologyBuilder();
        tp.setSpout("kafka_spout", getKafkaInputSpout(), 1);
        tp.setBolt("bolt_1", new KafkaTestBolt(), 2).shuffleGrouping("kafka_spout", STREAM_INPUT_TO_FIRST_BOLT);
        tp.setBolt("bolt_2", new KafkaTestBolt2(), 3).shuffleGrouping("bolt_1");
        tp.setBolt("kafka_output", getKafkaOutputBolt(), 1).shuffleGrouping("bolt_2");
        return tp.createTopology();
    }

    protected static final KafkaSpout getKafkaInputSpout() {
        return new KafkaSpout<>(getKafkaInputSpoutConfig());
    }

    protected static KafkaSpoutConfig<String, String> getKafkaInputSpoutConfig() {
        /**
         * @brief creates a kafkaspoutconfig object and return it
         * @param bootstrapServers: The bootstrap server ip
         * @note topic record translator will convert the messages into named tuples with the field names
         */
        ByTopicRecordTranslator<String, String> trans = new ByTopicRecordTranslator<>(
                (r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
                new Fields("topic", "partition", "offset", "key", "value"), STREAM_INPUT_TO_FIRST_BOLT);

        return KafkaSpoutConfig.builder(KAFKA_LOCAL_BROKER, new String[]{KAFKA_TOPIC_INPUT})
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_GROUP)
                .setRetry(getRetryService())
                .setRecordTranslator(trans)
                .setOffsetCommitPeriodMs(10000)
                .setFirstPollOffsetStrategy(UNCOMMITTED_LATEST)
                .setMaxUncommittedOffsets(250)
                .build();
    }

    protected static KafkaSpoutRetryService getRetryService() {
        return new KafkaSpoutRetryExponentialBackoff(TimeInterval.microSeconds(500),
                TimeInterval.milliSeconds(2), Integer.MAX_VALUE, TimeInterval.seconds(10));
    }

    protected static final KafkaBolt<String, String> getKafkaOutputBolt() {
        return new KafkaBolt<String, String>()
                .withProducerProperties(getKafkaOutputBoltProps())
                .withTopicSelector(new DefaultTopicSelector(KAFKA_TOPIC_OUTPUT))
                .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper<>(null, FINAL_OUTPUT_FIELD_NAME));
    }

    protected static Properties getKafkaOutputBoltProps() {
        return new Properties() {
            {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_LOCAL_BROKER);
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                put(ProducerConfig.CLIENT_ID_CONFIG, KAFKA_TOPIC_OUTPUT);
            }
        };
    }

}
