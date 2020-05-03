package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.TemperatureEvent;


public class TemperatureEsperOperation {
    private static final Logger LOG = LoggerFactory.getLogger(TemperatureEsperOperation.class);
    private EPRuntime cepRT = null;
    private static final String TEMPERATURE_TIME_WINDOW_BATCH = "30";
    private static final String TEMPERATURE_WARNING_EVENT_THRESHOLD = "20"; //TODO To be removed?
    private static final String TEMPERATURE_CRITICAL_EVENT_THRESHOLD = "10";
    private static final String TEMPERATURE_CRITICAL_EVENT_MULTIPLIER = "0.5";
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
                LOG.warn("ApacheStormMachine --> #################### Event received: " + newData);
                for (EventBean eventBean : newData) {
                    LOG.warn("ApacheStormMachine --> ************************ Event received 1: " + eventBean.getUnderlying());
                    LOG.warn("ApacheStormMachine --> ************************ " );

                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
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
                "providerUri", cepConfig);
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
                .append("Temperature.win:time_batch(")
                .append(TEMPERATURE_TIME_WINDOW_BATCH)
                .append(" sec)")
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
        warningQuery.append("select * from Temperature")
                .append(" match_recognize ( ")
                .append("       measures A as press1, B as press2 ")
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
        criticalQuery.append("select * from Temperature")
                .append(" match_recognize ( ")
                .append("       measures A as temp1, B as temp2, C as temp3, D as temp4 ")
                .append("       pattern (A B C D) ")
                .append("       define ")
                .append("               A as A.temperature > ")
                .append(TEMPERATURE_CRITICAL_EVENT_THRESHOLD)
                .append(",")
                .append("               B as (A.temperature < B.temperature), ")
                .append("               C as (B.temperature < C.temperature), ")
                .append("               D as (C.temperature < D.temperature) and D.temperature > (A.temperature * ")
                .append(TEMPERATURE_CRITICAL_EVENT_MULTIPLIER)
                .append("))");
        LOG.info("Critical Query for Temperature was set!");
        return  criticalQuery.toString();
    }
}