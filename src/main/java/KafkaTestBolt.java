
import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class KafkaTestBolt extends BaseRichBolt {

    private OutputCollector collector;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    private int process(String data) {
        return data.length();
    }

    @Override
    public void execute(Tuple input) {
        String topic = input.getStringByField("topic");
        int partition = input.getIntegerByField("partition");
        long offset = input.getLongByField("offset");
        String key = input.getStringByField("key");
        String value = input.getStringByField("value");

        int result = process(value);

        System.out.println(String.format("[AEGIS] [KafkaTestBolt] Data processed! IN=%s,%d,%d,%s,%s, OUT=%d",
                topic, partition, offset, key, value, result));
        collector.emit(new Values(result));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("length"));
    }

}
