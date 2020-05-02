package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.Events.PressureEvent;
import org.stormexample.Events.TemperatureEvent;
import org.stormexample.Events.VoltageEvent;

public class TemperatureEsperOperation {
    private static final Logger LOG = LoggerFactory.getLogger(TemperatureEsperOperation.class);
    private EPRuntime cepRT = null;
    private static final String WARNING_EVENT_THRESHOLD = "20"; //TODO To be removed?
    private static final String CRITICAL_EVENT_THRESHOLD = "10";
    private static final String CRITICAL_EVENT_MULTIPLIER = "0.5";
    private Configuration cepConfig = new Configuration();

    public TemperatureEsperOperation() {
        LOG.info("ApacheStormMachine --> Initializing service operations for Temperature variable");
        String averageQuery = queryGenerator(0);
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


    private String queryGenerator(int QueryType){ //TODO to beupdated with  generic queries :)
        StringBuilder createQuery = new StringBuilder();
        switch(QueryType){
            case 0:
                /**
                 * EPL to monitor the average temperature every 10 seconds. Will call listener on every event.
                 */

                createQuery./*append("select avg(temperature) from ") //Space in the end if needed :P; */
                        append("select avg(")
                        .append("temperature) from ")
                        .append("Temperature")
                        .append("");
                LOG.info("Average Query for Temperature was set!");
                break;
            case 1:
                /**
                 * EPL to check for 2 consecutive Temperature events over the threshold - if matched, will alert
                 * listener.
                 */
                createQuery.append("select * from Temperature")
                        //.append(EventTypeName)
                        .append(" match_recognize ( ")
                        .append("       measures A as temp1, B as temp2 ")
                        .append("       pattern (A B) ")
                        .append("       define ")
                        .append("               A as A.temperature > ")
                        .append(WARNING_EVENT_THRESHOLD)
                        .append(",")
                        .append("               B as B.temperature > ")
                        .append(WARNING_EVENT_THRESHOLD)
                        .append(")");
                LOG.info("Warning Query for Temperature was set!");
                break;
            case 2:
                /**
                 * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
                 * than the first event. This is checking for a sudden, sustained escalating rise in the
                 * temperature
                 */
                createQuery.append("select * from Temperature")
                       // .append(EventTypeName)
                        .append(" match_recognize ( ")
                        .append("       measures A as temp1, B as temp2, C as temp3, D as temp4 ")
                        .append("       pattern (A B C D) ")
                        .append("       define ")
                        .append("               A as A.temperature > ")
                        .append(CRITICAL_EVENT_THRESHOLD)
                        .append(",")
                        .append("               B as (A.temperature < B.temperature), ")
                        .append("               C as (B.temperature < C.temperature), ")
                        .append("               D as (C.temperature < D.temperature) and D.temperature > (A.temperature * ")
                        .append(CRITICAL_EVENT_MULTIPLIER)
                        .append("))");
                LOG.info("Critical Query for Temperature was set!");
                break;
            default:
                LOG.error("No Query was set!");
        }

        return createQuery.toString();
    }
}