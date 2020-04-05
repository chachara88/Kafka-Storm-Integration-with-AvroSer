package org.stormexample;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        int i =0;
        for(String s: parts) {
            LOG.info("ApacheStormMachine --> ### To part[" + i +"] einai " + s);
            i++;
        }
//        double price = input.getDoubleByField("price");
//        long timeStamp = input.getLongByField("timestamp");
//        //long timeStamp = System.currentTimeMillis();
//        String product = input.getStringByField("product");
//        Tuple stock = new Stock(product, price, timeStamp);
//        esperOperation.esperPut(stock);
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
