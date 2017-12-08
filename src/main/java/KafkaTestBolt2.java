import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class KafkaTestBolt2 extends BaseRichBolt {

    private OutputCollector collector;
    private Random rng;

    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.rng = new Random();
    }

    private int process(int data) {
        return rng.nextInt() / data;
    }

    @Override
    public void execute(Tuple input) {
        int value = input.getIntegerByField("length");
        int result = process(value);
        String output = String.format("{%s:%d}", "random_result", result);
        System.out.println(String.format("[AEGIS] [KafkaTestBolt2] Randomized a number! IN=%d, OUT=%s", value, output));
        collector.emit(new Values(output));
        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("output"));
    }

}
