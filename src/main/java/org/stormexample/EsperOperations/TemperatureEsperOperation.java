package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.TemperatureEvent;

import javax.management.RuntimeErrorException;


public class TemperatureEsperOperation {
    private static final Logger LOG = LoggerFactory.getLogger(TemperatureEsperOperation.class);
    private EPRuntime cepRT = null;
    private static final String TEMPERATURE_TIME_WINDOW_BATCH = "30";
    private static final String TEMPERATURE_WARNING_EVENT_THRESHOLD = "24.8";
    private static final String TEMPERATURE_CRITICAL_EVENT_THRESHOLD = "24.5";
    private static final String TEMPERATURE_CRITICAL_EVENT_ADDER = "0.1";
    private static final String TIME_MEASUREMENT_UNIT = " sec)";
    private static final String TIME_BATCH_WINDOW = "Temperature.win:time_batch(";
    private Configuration cepConfig = new Configuration();

    public TemperatureEsperOperation() {

    }

    public TemperatureEsperOperation(EsperStormTopology.Query eventQuery) {
        LOG.info("ApacheStormMachine --> Initializing service operations for Temperature variable");
        String averageQuery = queryGenerator(eventQuery);
        initializeService(averageQuery);
    }
    public static class CEPListener implements UpdateListener {

        public void update(EventBean[] newData, EventBean[] oldData) {
            try {
                for (EventBean eventBean : newData) {
                    LOG.warn("ApacheStormMachine --> ******* Temperature Event received *******: " + eventBean.getUnderlying());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
                throw new RuntimeException();
            }
        }
    }

    public void initializeService(String query){
        LOG.info("ApacheStormMachine --> Initializing service for monitoring  " + "Temperature events");
        cepConfig.addEventType("Temperature", TemperatureEvent.class.getName());
        executeQueries(cepConfig, query, "TemperatureUri");
    }

    public void esperPut(TemperatureEvent Temperature) { cepRT.sendEvent(Temperature); }

    private void executeQueries(Configuration cepConfig, String query, String providerUri){
        EPServiceProvider cep = EPServiceProviderManager.getProvider(
                providerUri, cepConfig);
        cepRT = cep.getEPRuntime();

        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm
                .createEPL(query);
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
         * EPL to monitor the pressure temperature every 10 seconds. Will call listener on every event.
         */
        averageQuery.append("select avg(")
                .append("temperature) from ")
                .append(TIME_BATCH_WINDOW)
                .append(TEMPERATURE_TIME_WINDOW_BATCH)
                .append(TIME_MEASUREMENT_UNIT)
                .append("");
        LOG.info("Average Query for Temperature  was set!");
        return  averageQuery.toString();
    }

    private String warningQuerybuilder() {
        StringBuilder warningQuery = new StringBuilder();
        /**
         * EPL to check for 2 consecutive temperature events over the threshold - if matched, will alert
         * listener.
         */
        warningQuery.append("select * from ")
                .append(TIME_BATCH_WINDOW)
                .append(TEMPERATURE_TIME_WINDOW_BATCH)
                .append(TIME_MEASUREMENT_UNIT)
                .append(" match_recognize ( ")
                .append("       measures A as temp1, B as temp2 ")
                .append("       pattern (A B) ")
                .append("       define ")
                .append("               A as A.temperature > ")
                .append(TEMPERATURE_WARNING_EVENT_THRESHOLD)
                .append(",")
                .append("               B as B.temperature > ")
                .append(TEMPERATURE_WARNING_EVENT_THRESHOLD)
                .append(")");
        LOG.info("Warning Query for Temperature was set!");
        return  warningQuery.toString();
    }

    private String criticalQuerybuilder() {
        StringBuilder criticalQuery = new StringBuilder();
        /**
         * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
         * than the first event. This is checking for a sudden, sustained escalating rise in the
         * temperature
         */
        criticalQuery.append("select * from ")
                .append(TIME_BATCH_WINDOW)
                .append(TEMPERATURE_TIME_WINDOW_BATCH)
                .append(TIME_MEASUREMENT_UNIT)
                .append(" match_recognize ( ")
                .append("       measures A as temp1, B as temp2, C as temp3, D as temp4 ")
                .append("       pattern (A B C D) ")
                .append("       define ")
                .append("               A as A.temperature > ")
                .append(TEMPERATURE_CRITICAL_EVENT_THRESHOLD)
                .append(",")
                .append("               B as (A.temperature <= B.temperature), ")
                .append("               C as (B.temperature <= C.temperature), ")
                .append("               D as (C.temperature <= D.temperature) and D.temperature >= (A.temperature + ")
                .append(TEMPERATURE_CRITICAL_EVENT_ADDER)
                .append("))");
        LOG.info("Critical Query for Temperature was set!");
        return  criticalQuery.toString();
    }
}