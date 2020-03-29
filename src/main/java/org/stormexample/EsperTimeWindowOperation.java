package org.stormexample;

import com.espertech.esper.client.*;

/*The constructor of the EsperOperation class
/*initializes the Esper listener and sets the Esper query.
/*The Esper query buffers the events over 5 minutes and
/*returns the result of the calculation during the 5 minutes window.
Here, we are using the fixed batch window.*/

public class EsperTimeWindowOperation {
    private EPRuntime cepRT = null;
    public EsperTimeWindowOperation() {
        Configuration cepConfig = new Configuration();
      //  cepConfig.addEventType("VoltageValue", MessageBean.class.getName());
        EPServiceProvider cep = EPServiceProviderManager.getProvider(
                "ApacheStormCEPEngine", cepConfig);
        cepRT = cep.getEPRuntime();

        EPAdministrator cepAdm = cep.getEPAdministrator();
        EPStatement cepStatement = cepAdm
                .createEPL(""); /*TODO be filled with Esper query!*/

        cepStatement.addListener(new CEPListener());
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

//    public void esperPut(Stock stock) {
//        cepRT.sendEvent(stock);
//    }
//

}
