package org.stormexample;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public class SourceModuleExtractor implements IBasicBolt {
        private static final Logger LOG = LoggerFactory.getLogger(AvroKafkaSpout.class);
        public SourceModuleExtractor(){}

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
    /*TODO needs to be more sophisticated :)*/
        if (tuple.contains("value")) {
            String part = tuple.toString();
            String[] parts = part.split(" ");
            //parts[14] is the sourceModule
            //TODO a seperate function fro logic below
            if (parts[14].contains("BAROMETER-TEMPERATURE")) {
                LOG.error("ApacheStormMachine --> Temperature Value will be redirected in TemperatureStream id"); //TODO
                basicOutputCollector.emit("TemperatureStream", new Values(parts[18]));
            } else if (parts[14].contains("BAROMETER-PRESSURE")) {
                LOG.error("ApacheStormMachine --> Pressure Value will be redirected in PressureStream id"); //TODO
                basicOutputCollector.emit("PressureStream", new Values(parts[18]));
            } else {
                basicOutputCollector.emit(new Values(parts[18])); //TODO Special case. It should be handled, despite that it is not expected in our scenario.
            }
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
        outputFieldsDeclarer.declareStream("TemperatureStream", new Fields("TemperatureValue") );
        outputFieldsDeclarer.declareStream("PressureStream", new Fields("PressureValue") );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
