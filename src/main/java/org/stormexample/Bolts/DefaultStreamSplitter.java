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
        LOG.error("ApacheStormMachine --> Emitted tuple IN source module extractor BOLT  is: [{}]", tuple);
        tuple.getFields();
        String streamId = "";
        if (tuple.contains("value")) {  /*TODO needs to be more sophisticated :)*/
            String part = tuple.toString();
            String[] parts = part.split(" ");
            /*parts[14] is the sourceModule && parts[18] is variables value*/
            //TODO a seperate function fro logic below
            if (parts[14].contains("BAROMETER-TEMPERATURE")) {
                LOG.error("ApacheStormMachine --> Temperature Value will be redirected in TemperatureStream id"); //TODO
                streamId = "TemperatureStream";
            } else if (parts[14].contains("BAROMETER-PRESSURE")) {
                LOG.error("ApacheStormMachine --> Pressure Value will be redirected in PressureStream id"); //TODO
                streamId = "PressureStream";
            }
            else{
            //TODO special handling is needed. Throw an exception?
                LOG.error("ApacheStormMachine --> There is NO Temperature or Pressure Stream"); //TODO
            }
            basicOutputCollector.emit(streamId, tuple.getValues());
        }
    }

    @Override
    public void cleanup() {

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
        * The tuple schema will still remain the same as in teh default.
        * TODO this has to be changed so as to avoid extra manipulation in the next bolt
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
