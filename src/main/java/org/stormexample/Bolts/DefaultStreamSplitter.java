package org.stormexample.Bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public class DefaultStreamSplitter implements IBasicBolt {
        private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamSplitter.class);
        public DefaultStreamSplitter(){}

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext) {
        try {

        } catch (Exception e) {

        }
    }

    /**
     * @param tuple
     * @param basicOutputCollector
     */
    @Override

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.info("ApacheStormMachine --> Emitted tuple in stream splitter BOLT  is: [{}]", tuple);
        tuple.getFields();
        if (tuple.contains("value")) {
            String part = tuple.toString();
            String[] parts = part.split(" ");
            String streamId = redirectoToSpecificStream(parts);
            basicOutputCollector.emit(streamId, tuple.getValues());
        }
    }

    @Override
    public void cleanup() {
    }

    private String redirectoToSpecificStream(String[] tupleParts){
        /*parts[14] is the sourceModule && parts[18] is variables value*/
        if (tupleParts[14].contains("BAROMETER-TEMPERATURE")) {
            LOG.warn("ApacheStormMachine --> Temperature tuple (value) will be redirected in TemperatureStream");
            return "TemperatureStream";
        } else if (tupleParts[14].contains("BAROMETER-PRESSURE")) {
            LOG.warn("ApacheStormMachine --> Pressure tuple (value) will be redirected in PressureStream ");
            return  "PressureStream";
        }
        else{
            LOG.error("ApacheStormMachine --> There is NO Temperature or Pressure Stream");
            return "";
        }
    }

    /**
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        /*Break the default stream into two seperate ones based on variable
        * The two new streams are:
        * i.  TemperatureStream
        * ii. PressureStream
        * The tuple schema will still remain the same as in the default.
        * */
        Fields schema = new Fields("topic","partition", "offset", "key", "value");
        outputFieldsDeclarer.declareStream("TemperatureStream", schema );
        outputFieldsDeclarer.declareStream("PressureStream", schema );
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
