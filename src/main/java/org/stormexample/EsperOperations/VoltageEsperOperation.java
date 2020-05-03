package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.VoltageEvent;

public class VoltageEsperOperation {
    private static final Logger LOG = LoggerFactory.getLogger(VoltageEsperOperation.class);
    private EPRuntime cepRT = null;
    private static final String VOLTAGE_TIME_WINDOW_BATCH = "15";
    private static final String VOLTAGE_WARNING_EVENT_THRESHOLD = "10"; //TODO To be removed?
    private static final String VOLTAGE_CRITICAL_EVENT_THRESHOLD = "9";
    private static final String VOLTAGE_CRITICAL_EVENT_MULTIPLIER = "1";
    private Configuration cepConfig = new Configuration();

    public VoltageEsperOperation() {}

    public VoltageEsperOperation(EsperStormTopology.Query query) {
        LOG.info("ApacheStormMachine --> Initializing service operations for Voltage variable");
        String averageQuery = queryGenerator(query);
        initializeService(averageQuery);
    }


    public static class CEPListener implements UpdateListener {

        public void update(EventBean[] newData, EventBean[] oldData) { //TODO to be updated!
            try {
                for (EventBean eventBean : newData) {
                    LOG.warn("ApacheStormMachine --> ******* Voltage Event received *******: " + eventBean.getUnderlying());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        }
    }

    public void initializeService(String query){
        LOG.info("ApacheStormMachine --> Initializing service for monitoring " + "Voltage events");
        cepConfig.addEventType("Voltage", VoltageEvent.class.getName());
        executeQueries(cepConfig, query,"VoltageUri");
    }

    public void esperPut(VoltageEvent Voltage) { cepRT.sendEvent(Voltage); }

    private void executeQueries(Configuration cepConfig, String query, String providerUri){
        EPServiceProvider cep = EPServiceProviderManager.getProvider("providerUri", cepConfig);
        cepRT = cep.getEPRuntime();
        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        cepStatement.addListener(new CEPListener());
    }

    private String queryGenerator(EsperStormTopology.Query QueryType){
        String createQuery = "";
        switch(QueryType){
            case AVERAGE:
                createQuery = averageQuerybuilder();
                break;
            case WARNING:
                createQuery = warningQuerybuilder();
                break;
            case CRITICAL:
                createQuery = criticalQuerybuilder();
                break;
            default:
                LOG.error("No Query was set!");
        }

        return createQuery;
    }

    private String averageQuerybuilder() {
        StringBuilder averageQuery = new StringBuilder();
        /**
         * EPL to monitor the voltage every 10 seconds. Will call listener on every event.
         */
        averageQuery.append("select avg(")
                .append("voltage) from ")
                .append("Voltage.win:time_batch(")
                .append(VOLTAGE_TIME_WINDOW_BATCH)
                .append(" sec)")
                .append("");
        LOG.info("Average Query for Voltage  was set!");
        return  averageQuery.toString();
    }

    private String warningQuerybuilder() {
        StringBuilder warningQuery = new StringBuilder();
        /**
         * EPL to check for 2 consecutive voltage events over the threshold - if matched, will alert
         * listener.
         */
        warningQuery.append("select * from ")
                .append("Voltage.win:time_batch(")
                .append(VOLTAGE_TIME_WINDOW_BATCH)
                .append(" sec)")
                .append(" match_recognize ( ")
                .append("       measures A as volt1, B as volt2 ")
                .append("       pattern (A B) ")
                .append("       define ")
                .append("               A as A.voltage > ")
                .append(VOLTAGE_WARNING_EVENT_THRESHOLD)
                .append(",")
                .append("               B as B.voltage > ")
                .append(VOLTAGE_WARNING_EVENT_THRESHOLD)
                .append(")");
        LOG.info("Warning Query for Voltage was set!");
        return  warningQuery.toString();
    }

    private String criticalQuerybuilder() {
        StringBuilder criticalQuery = new StringBuilder();
        /**
         * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
         * than the first event. This is checking for a sudden, sustained escalating rise in the
         * voltage
         */
        criticalQuery.append("select * from ")
                .append("Voltage.win:time_batch(")
                .append(VOLTAGE_TIME_WINDOW_BATCH)
                .append(" sec)")
                .append(" match_recognize ( ")
                .append("       measures A as volt1, B as volt2, C as volt3, D as volt4 ")
                .append("       pattern (A B C D) ")
                .append("       define ")
                .append("               A as A.voltage > ")
                .append(VOLTAGE_CRITICAL_EVENT_THRESHOLD)
                .append(",")
                .append("               B as (A.voltage <= B.voltage), ")
                .append("               C as (B.voltage <= C.voltage), ")
                .append("               D as (C.voltage <= D.voltage) and D.voltage >= (A.voltage * ")
                .append(VOLTAGE_CRITICAL_EVENT_MULTIPLIER)
                .append("))");
        LOG.info("Critical Query for Voltage was set!");
        return  criticalQuery.toString();
    }
}