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

public class testerEsperBolt implements IBasicBolt {
        private static final Logger LOG = LoggerFactory.getLogger(AvroKafkaSpout.class);
        public testerEsperBolt(){}

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext) {
        try {

        } catch (Exception e) {

        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        LOG.error("ApacheStormMachine --> Emitted tuple IN BOLT  is: [{}]", tuple);
        tuple.getFields();
        if (tuple.contains("value")){
            String part = tuple.toString();
            String[] parts = part.split(" ");
            //part[18] is referring to actual Value!
            basicOutputCollector.emit(new Values(parts[18]));
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("VoltageValue"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
