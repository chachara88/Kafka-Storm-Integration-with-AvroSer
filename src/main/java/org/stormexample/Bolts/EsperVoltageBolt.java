package org.stormexample.Bolts;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperOperations.EsperMonitorValueOperation;
import org.stormexample.Events.TemperatureEvent;

import java.time.LocalTime;
import java.util.Map;


public class EsperVoltageBolt implements IBasicBolt {
    private static final Logger LOG = LoggerFactory.getLogger(EsperVoltageBolt.class);
    private static final long serialVersionUID = 2L;
    private EsperMonitorValueOperation esperOperation;

    public EsperVoltageBolt() {
    }

    public void execute(Tuple input, BasicOutputCollector collector) {

        LOG.info("ApacheStormMachine --> ### Exeucte in EsperVoltage Bolt\n");
        LOG.info("ApacheStormMachine --> ### Tuple is [{}]\n", input);
        int x = input.getFields().size();
        LOG.info("ApacheStormMachine --> ### To size twn fields einai: "+x );
        String part = input.toString();
        String[] parts = part.split(" ");
        if(parts[7] != null){
            try {
                LOG.info("ApacheStormMachine --> String will be formatted!");
                String stringValue = parts[7].substring(0,parts[7].length() - 1); //Trim last character (comma in our case)
                TemperatureEvent temperatureEvent = new TemperatureEvent(Double.parseDouble(stringValue.trim()),LocalTime.now());
                esperOperation.esperPut(temperatureEvent);
            }catch(NumberFormatException ex){
                LOG.error("ApacheStormMachine --> Invalid string!! Please use a  well-formatted string :)");
            }
        }else{
            LOG.info("ApacheStormMachine --> String is null!!!");
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
            // create the instance of ESOperations class
            esperOperation = new EsperMonitorValueOperation();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public void cleanup() {

    }

}
