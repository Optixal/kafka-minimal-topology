package Other;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class KafkaTopologyBackup {

    private static final String KAFKA_LOCAL_BROKER = "localhost:9092";

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_spout", new KafkaSpout<>(
                KafkaSpoutConfig.builder("127.0.0.1:9092", "testing")
                        .setProp(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_LOCAL_BROKER)
                        .setProp(ConsumerConfig.GROUP_ID_CONFIG, "kafkaSpoutTestGroup")
                        .build()),
                1
        );
        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("kafka_spout");
        builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(false);

        // Remote - Command Line storm
        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        // Local
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("KafkaTopology", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }

    }
}
