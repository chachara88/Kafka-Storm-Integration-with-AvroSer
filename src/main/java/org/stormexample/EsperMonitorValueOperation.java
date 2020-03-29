package org.stormexample;

import com.espertech.esper.client.*;
import org.apache.storm.tuple.Tuple;

import java.util.Random;

public class EsperMonitorValueOperation {
    private EPRuntime cepRT = null;
    public EsperMonitorValueOperation() {
        Configuration cepConfig = new Configuration();
        cepConfig.addEventType("VoltageValue", Tuple.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(
                "ApacheStormCEPEngine", cepConfig);
        cepRT = cep.getEPRuntime();

        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm
                .createEPL(""); /*TODO To be filled with Esper Query*/

        cepStatement.addListener(new EsperTimeWindowOperation.CEPListener());
    }

    public static class CEPListener implements UpdateListener {

        public void update(EventBean[] newData, EventBean[] oldData) {
            try {
                System.out.println("#################### Event received: "+newData);
                for (EventBean eventBean : newData) {
                    System.out.println("************************ Event received 1: " + eventBean.getUnderlying());
                }

            } catch (Exception e) {
                e.printStackTrace();
                System.out.println(e);
            }
        }
    }

    public void esperPut(Tuple SingleValue) {
        cepRT.sendEvent(SingleValue);
    }

    }
