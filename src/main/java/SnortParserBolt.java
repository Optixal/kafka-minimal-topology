import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class SnortParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap csvConfig = new HashMap();  /**< key, value pair for csv parser config **/
    private DateTimeFormatter format;


    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        csvConfig.put("source.type", -1);
        csvConfig.put("timestamp",    1);
        csvConfig.put("ip_src_addr",  2);
        csvConfig.put("ip_src_port",  3);
        csvConfig.put("ip_dst_addr",  4);
        csvConfig.put("ip_dst_port",  5);
        csvConfig.put("protocol",     6);

        csvConfig.put("ethsrc",       7);
        csvConfig.put("ethdst",       8);

        format = DateTimeFormatter.ofPattern("MM/dd/yy-HH:mm:ss.SSSSSS").withZone(ZoneId.systemDefault());
        /* Prepare the date time formatter object to parse the datetime format of snort */
    }

    @Override
    public void execute(Tuple input) {
        String[] csvFields = input.getStringByField("value").split(",");
        String[] output = new String[csvConfig.values().size()];

        for (Object i: csvConfig.values()) {
            if ((int) i != -1)
                output[(int) i] = csvFields[(int) i];
            /** -1 is a filter statement for custom value mapping */
        }

        output[0] = "snort";
        /* source.type to snort */
        output[1] = String.valueOf(date2epoch(csvFields[0]));
        /* Convert timestamp to epoch time */

        System.out.print("[AEGIS]" + Arrays.toString(output));
        collector.emit(new Values(output));
        collector.ack(input);
    }

    public long date2epoch(String dateStr) {
        /**
         * @brief Converts date format in string to epoch time
         * @param dateStr   the string containing the date
         * @retval long epoch timestamp
         */
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(dateStr.trim(), format);
        return zonedDateTime.toInstant().toEpochMilli();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new ArrayList<String>(csvConfig.keySet())));
    }

}
