package org.stormexample.Bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperOperations.PressureEsperOperation;
import org.stormexample.EsperOperations.TemperatureEsperOperation;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.PressureEvent;

import javax.management.Query;
import java.time.LocalTime;
import java.util.Map;


public class PressureEsperBolt implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(PressureEsperBolt.class);
    private static final long serialVersionUID = 2L;
    private PressureEsperOperation esperOperation;
    private EsperStormTopology.Query EventQuery;
    public PressureEsperBolt() {
    }

    public PressureEsperBolt(EsperStormTopology.Query query) {
        this.EventQuery = query;
    }

    public void execute(Tuple input, BasicOutputCollector collector) {

        LOG.info("ApacheStormMachine --> In execute in PressureEsperBolt \n");
        String part = input.toString();
        String[] parts = part.split(" ");
        if(parts[7] != null){
            try {
                LOG.warn("ApacheStormMachine --> String will be formatted!");
                String stringValue = parts[7].substring(0,parts[7].length() - 1); //Trim last character (comma in our case)
                PressureEvent pressureEvent = new PressureEvent(Double.parseDouble(stringValue.trim()),LocalTime.now());
                esperOperation.esperPut(pressureEvent);
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
            esperOperation = new PressureEsperOperation(EventQuery);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public void cleanup() {

    }

}
