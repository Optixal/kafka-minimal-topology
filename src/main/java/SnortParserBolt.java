import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SnortParserBolt extends BaseRichBolt {

    private OutputCollector collector;
    private HashMap csvConfig = new HashMap();  /**< key, value pair for csv parser config **/
    private DateTimeFormatter format;


    @Override
    public void prepare(Map topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        csvConfig.put("timestamp",    0);
        csvConfig.put("ip_src_addr",  1);
        csvConfig.put("ip_src_port",  2);
        csvConfig.put("ip_dst_addr",  3);
        csvConfig.put("ip_dst_port",  4);
        csvConfig.put("protocol",     5);
        csvConfig.put("ethsrc",       6);
        csvConfig.put("ethdst",       7);
        csvConfig.put("source.type", -1);
        /** TODO move source.type to first index in output */

        format = DateTimeFormatter.ofPattern("MM/dd/yy-HH:mm:ss.SSSSSS").withZone(ZoneId.systemDefault());
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
        output[0] = String.valueOf(date2epoch(csvFields[0]));
        /** if it is timestamp, convert it to epoch first */
        output[output.length - 1] = "snort";

        System.out.print("OUTPUT:");
        for (String str: output) {
            System.out.print(str);
        }
        System.out.println();
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
