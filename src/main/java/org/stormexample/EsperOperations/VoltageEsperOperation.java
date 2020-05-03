package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.EsperStormTopology;
import org.stormexample.Events.VoltageEvent;

public class VoltageEsperOperation {
    private static final Logger LOG = LoggerFactory.getLogger(VoltageEsperOperation.class);
    private EPRuntime cepRT = null;
    private static final String VOLTAGE_WARNING_EVENT_THRESHOLD = "20"; //TODO To be removed?
    private static final String VOLTAGE_CRITICAL_EVENT_THRESHOLD = "10";
    private static final String VOLTAGE_CRITICAL_EVENT_MULTIPLIER = "0.5";
    private Configuration cepConfig = new Configuration();

    public VoltageEsperOperation() {}

    public VoltageEsperOperation(EsperStormTopology.Query query) {
        LOG.info("ApacheStormMachine --> Initializing service operations for Voltage variable");
//        //TODO  To be updated with Temperature Event
        String averageQuery = queryGenerator(query);
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
        LOG.info("ApacheStormMachine --> Initializing service for monitoring " + "Voltage events");
        cepConfig.addEventType("Voltage", VoltageEvent.class.getName());
        executeQueries(cepConfig, query,"VoltageUri");
    }

    public void esperPut(VoltageEvent Voltage) { cepRT.sendEvent(Voltage); }

    private void executeQueries(Configuration cepConfig, String query, String providerUri){
        EPServiceProvider cep = EPServiceProviderManager.getProvider(
                "providerUri", cepConfig);
        cepRT = cep.getEPRuntime();

        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm
                .createEPL(query);
        cepStatement.addListener(new CEPListener());
    }


    private String queryGenerator(EsperStormTopology.Query queryType){
        StringBuilder createQuery = new StringBuilder();
        switch(queryType){
            case AVERAGE:
                /**
                 * EPL to monitor the average Voltage every 10 seconds. Will call listener on every event.
                 */

                createQuery./*append("select avg(temperature) from ") //Space in the end if needed :P; */
                        append("select avg(")
                        .append("voltage) from ")
                        .append("Voltage")
                        .append("");
                LOG.info("Average Query for Voltage was set!");
                break;
            case WARNING:
                /**
                 * EPL to check for 2 consecutive Voltage events over the threshold - if matched, will alert
                 * listener.
                 */
                createQuery.append("select * from Voltage")
                        //.append(EventTypeName)
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
                break;
            case CRITICAL:
                /**
                 * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
                 * than the first event. This is checking for a sudden, sustained escalating rise in the
                 * voltage
                 */
                createQuery.append("select * from Voltage")
                        // .append(EventTypeName)
                        .append(" match_recognize ( ")
                        .append("       measures A as volt1, B as volt2, C as volt3, D as volt4 ")
                        .append("       pattern (A B C D) ")
                        .append("       define ")
                        .append("               A as A.voltage > ")
                        .append(VOLTAGE_CRITICAL_EVENT_THRESHOLD)
                        .append(",")
                        .append("               B as (A.voltage < B.voltage), ")
                        .append("               C as (B.voltage < C.voltage), ")
                        .append("               D as (C.voltage < D.voltage) and D.voltage > (A.voltage * ")
                        .append(VOLTAGE_CRITICAL_EVENT_MULTIPLIER)
                        .append("))");
                LOG.info("Critical Query for Voltage was set!");
                break;
            default:
                LOG.error("No Query was set!");
        }
        return createQuery.toString();
    }
}