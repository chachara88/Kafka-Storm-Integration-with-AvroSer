package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.PressureEvent;


public class PressureEsperOperation {
    private static final Logger LOG = LoggerFactory.getLogger(PressureEsperOperation.class);
    private EPRuntime cepRT = null;
    private static final String PRESSURE_WARNING_EVENT_THRESHOLD = "20"; //TODO To be removed?
    private static final String PRESSURE_TIME_WINDOW_BATCH = "30"; //TODO To be removed?
    private static final String PRESSURE_CRITICAL_EVENT_THRESHOLD = "10";
    private static final String PRESSURE_CRITICAL_EVENT_MULTIPLIER = "0.5";
    private Configuration cepConfig = new Configuration();


    public PressureEsperOperation() {
    }

    public PressureEsperOperation(EsperStormTopology.Query eventQuery) {
        LOG.info("ApacheStormMachine --> Initializing service operations for Pressure variable");
        String averageQuery = queryGenerator(eventQuery);
        initializeService(averageQuery);
    }



    public static class CEPListener implements UpdateListener {

        public void update(EventBean[] newData, EventBean[] oldData) { //TODO to be updated!
            try { //TODO to be updated in generic form:)`
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
        LOG.info("ApacheStormMachine --> Initializing service for monitoring .." + "Pressure events");
        cepConfig.addEventType("Pressure", PressureEvent.class.getName());
        executeQueries(cepConfig, query,"PressureUri");
    }

    public void esperPut(PressureEvent Pressure) { cepRT.sendEvent(Pressure); }

    private void executeQueries(Configuration cepConfig, String query, String providerUri){
        EPServiceProvider cep = EPServiceProviderManager.getProvider("providerUri", cepConfig);
        cepRT = cep.getEPRuntime();

        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm.createEPL(query);
        cepStatement.addListener(new CEPListener());
    }


    private String queryGenerator(EsperStormTopology.Query QueryType){
        StringBuilder createQuery = new StringBuilder();
        switch(QueryType){
            case AVERAGE:
                /**
                 * EPL to monitor the pressure temperature every 10 seconds. Will call listener on every event.
                 */
                createQuery.append("select avg(")
                        .append("pressure) from ")
                        .append("Pressure.win:time_batch(")
                        .append(PRESSURE_TIME_WINDOW_BATCH)
                        .append(" sec)")
                        .append("");
                LOG.info("Average Query for Pressure  was set!");
                break;
            case WARNING:
                /**
                 * EPL to check for 2 consecutive pressure events over the threshold - if matched, will alert
                 * listener.
                 */
                createQuery.append("select * from Pressure")
//                        .append(EventTypeName)
                        .append(" match_recognize ( ")
                        .append("       measures A as press1, B as press2 ")
                        .append("       pattern (A B) ")
                        .append("       define ")
                        .append("               A as A.pressure > ")
                        .append(PRESSURE_WARNING_EVENT_THRESHOLD)
                        .append(",")
                        .append("               B as B.pressure > ")
                        .append(PRESSURE_WARNING_EVENT_THRESHOLD)
                        .append(")");
                LOG.info("Warning Query for Pressure was set!");
                break;
            case CRITICAL:
                /**
                 * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
                 * than the first event. This is checking for a sudden, sustained escalating rise in the
                 * pressure
                 */
                createQuery.append("select * from Pressure")
                        // .append(EventTypeName)
                        .append(" match_recognize ( ")
                        .append("       measures A as press1, B as press2, C as press3, D as press4 ")
                        .append("       pattern (A B C D) ")
                        .append("       define ")
                        .append("               A as A.pressure > ")
                        .append(PRESSURE_CRITICAL_EVENT_THRESHOLD)
                        .append(",")
                        .append("               B as (A.pressure < B.pressure), ")
                        .append("               C as (B.pressure < C.pressure), ")
                        .append("               D as (C.pressure < D.pressure) and D.pressure > (A.pressure * ")
                        .append(PRESSURE_CRITICAL_EVENT_MULTIPLIER)
                        .append("))");
                LOG.info("Critical Query for Pressure was set!");
                break;
            default:
                LOG.error("No Query was set!");
        }

        return createQuery.toString();
    }
}