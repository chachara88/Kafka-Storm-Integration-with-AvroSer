package org.stormexample.Bolts;

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

public class ValueExtractorBolt implements IBasicBolt {
        private static final Logger LOG = LoggerFactory.getLogger(ValueExtractorBolt.class);
        public ValueExtractorBolt(){}

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext) {
        try {

        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.info("ApacheStormMachine --> Emitted tuple in BOLT  is: [{}]", tuple);
        tuple.getFields();
        if (tuple.contains("value")){
            String part = tuple.toString();
            String extracted = part.substring(part.indexOf("\"value"),part.lastIndexOf('}'));
            basicOutputCollector.emit(new Values(extracted));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ExtractedValue"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
