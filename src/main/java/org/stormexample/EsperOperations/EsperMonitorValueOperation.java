package org.stormexample.EsperOperations;

import com.espertech.esper.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.Events.TemperatureEvent;

public class EsperMonitorValueOperation {
    private static final Logger LOG = LoggerFactory.getLogger(EsperMonitorValueOperation.class);
    private EPRuntime cepRT = null;
    private static final String WARNING_EVENT_THRESHOLD = "400"; //TODO To be removed?
    private static final String CRITICAL_EVENT_THRESHOLD = "10";
    private static final String CRITICAL_EVENT_MULTIPLIER = "0.5";

    public EsperMonitorValueOperation() {
        LOG.info("ApacheStormMachine --> Initializing service ..\n");
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("Temperature", TemperatureEvent.class.getName()); //TODO  To be updated with Temperature Event
        EPServiceProvider cep = EPServiceProviderManager.getProvider(
                "ApacheStormCEPEngine", cepConfig);
        cepRT = cep.getEPRuntime();

        EPAdministrator cepAdm = cep.getEPAdministrator();
        /**
         * EPL to check for 2 consecutive Temperature events over the threshold - if matched, will alert
         * listener.
         */
        String warningEventExpression = "select * from Temperature "
                + "match_recognize ( "
                + "       measures A as temp1, B as temp2 "
                + "       pattern (A B) "
                + "       define "
                + "               A as A.temperature > " + WARNING_EVENT_THRESHOLD + ", "
                + "               B as B.temperature > " + WARNING_EVENT_THRESHOLD + ")";

        /**
         * EPL to check for a sudden critical rise across 4 events, where the last event is 1.5x greater
         * than the first event. This is checking for a sudden, sustained escalating rise in the
         * temperature
         */
        String criticalEventExpression = "select * from Temperature "
                + "match_recognize ( "
                + "       measures A as temp1, B as temp2, C as temp3, D as temp4 "
                + "       pattern (A B C D) "
                + "       define "
                + "               A as A.temperature > " + CRITICAL_EVENT_THRESHOLD + ", "
                + "               B as (A.temperature < B.temperature), "
                + "               C as (B.temperature < C.temperature), "
                + "               D as (C.temperature < D.temperature) and D.temperature > (A.temperature * " + CRITICAL_EVENT_MULTIPLIER + ")" + ")";

        /**
         * EPL to monitor the average temperature every 10 seconds. Will call listener on every event.
         */
        String averageExpression = "select avg(temperature) from Temperature";

        EPStatement cepStatement = cepAdm
                  .createEPL(averageExpression); //TODO to be filled with the correct Queries
        cepStatement.addListener(new CEPListener()); //TODO to be deleted :)
    }

    public static class CEPListener implements UpdateListener {

        public void update(EventBean[] newData, EventBean[] oldData) {
            try {
                LOG.info("ApacheStormMachine --> #################### Event received: " + newData);
                for (EventBean eventBean : newData) {
                    LOG.info("ApacheStormMachine --> ************************ Event received 1: " + eventBean.getUnderlying());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        }
    }

    public void esperPut(/*TODO Tuple SingleValue*/ TemperatureEvent Temperature) {
        cepRT.sendEvent(Temperature);
    }

}