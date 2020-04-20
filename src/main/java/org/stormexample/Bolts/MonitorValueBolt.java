package org.stormexample.Bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperOperations.EsperMonitorValueOperation;

import java.util.Map;

public class MonitorValueBolt implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(MonitorValueBolt.class);
    private static final long serialVersionUID = 2L;
    private EsperMonitorValueOperation esperOperation;

    public MonitorValueBolt() {


    }


    public void execute(Tuple input, BasicOutputCollector collector) {
        LOG.error("ApacheStormMachine --> Emitted tuple IN MonitorValueBoltBOLT  is: [{}]", input);
    //TODO TO be filled accordingly
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    public void prepare(Map stormConf, TopologyContext context) {
        //TODO To be filled accordingly
    }

    public void cleanup() {

    }
}
