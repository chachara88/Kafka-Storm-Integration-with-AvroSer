package org.stormexample.Bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperOperations.VoltageEsperOperation;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.VoltageEvent;

import java.time.LocalTime;
import java.util.Map;


public class VoltageEsperBolt implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(VoltageEsperBolt.class);
    private static final long serialVersionUID = 2L;
    private VoltageEsperOperation esperOperation;
    private EsperStormTopology.Query eventQuery;

    public VoltageEsperBolt() {
    }

    public VoltageEsperBolt(EsperStormTopology.Query query) {
        this.eventQuery = query;
    }
    public void execute(Tuple input, BasicOutputCollector collector) {

        LOG.info("ApacheStormMachine --> In execute in VoltageEsperBolt\n");
        String part = input.toString();
        String[] parts = part.split(" ");
        if(parts[7] != null){
            try {
                LOG.info("ApacheStormMachine --> String will be formatted!");
                String stringValue = parts[7].substring(0,parts[7].length() - 1); //Trim last character (comma in our case)
                VoltageEvent voltageEvent = new VoltageEvent(Double.parseDouble(stringValue.trim()),LocalTime.now());
                esperOperation.esperPut(voltageEvent);
            }catch(NumberFormatException ex){
                LOG.error("ApacheStormMachine --> Invalid string!! Please use a  well-formatted string :)");
            }
        }else{
            LOG.warn("ApacheStormMachine --> String is null!!!");
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    public void prepare(Map stormConf, TopologyContext context) {
        try {
            esperOperation = new VoltageEsperOperation(eventQuery);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public void cleanup() {

    }

}
