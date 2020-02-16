package org.stormexample;

import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;


public class simplestormTopology {
    public static void main(String[] args) throws Exception{
        /*private static */ final Logger LOG = LoggerFactory.getLogger(simplestormTopology.class);
        //Wildcard topics
        // tp.setSpout("kafka_spout", new KafkaSpout<>(KafkaSpoutConfig.builder("127.0.0.1:" + port, Pattern.compile("topic.*")).build()), 1);
        Set<String> setOfTopics = new HashSet<String>(); //TODO: this can be a "setter" method outside main
        setOfTopics.add("aeroloop_AngularVelocity");
        setOfTopics.add("aeroloop_CpuUsage");
        setOfTopics.add("aeroloop_Current");
        setOfTopics.add("aeroloop_FuelUsage");
        setOfTopics.add("aeroloop_Location");
        setOfTopics.add("aeroloop_SensorReadingScalar");
        setOfTopics.add("aeroloop_StorageUsage");
        setOfTopics.add("aeroloop_UAVState");
        setOfTopics.add("aeroloop_UaVState");
        setOfTopics.add("aeroloop_Voltage");

        LOG.info("KAFKASPOUT: Configuring the KafkaSpout");

//        KafkaSpoutConfig.builder("127.0.0.1",setOfTopics); //it was replaced by the following
//        KafkaSpoutConfig.builder("localhost:6667", Pattern.compile("topic.*"))
//                .setOffsetCommitPeriodMs(10000) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
//                .setMaxUncommittedOffsets(259) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
//                /* processing guarantee mode is set to AT_LEAST_ONCE
//                *  that means that many methods are set to default,
//                *  eg: setEmitNullTuples(), setProcessingGuarantee(),
//                *  setTupleTrackingEnforced()
//                *  */
////                .setRecordTranslator() //TODO to be set accordingly
//                .build();
        //(new ZkHosts("tobechanged"), /*setOfTopics*/ aeroloop_AngularVelocity_topic, "/"+aeroloop_AngularVelocity_topic, UUID.randomUUID().toString()); ///*TODO change the IPaddr*/ /*Can I use many opics? instead of multiple spouts?*/
        KafkaSpout kafkaSpout1 = new KafkaSpout( KafkaSpoutConfig.builder("localhost:6667", Pattern.compile("topic.*"))
                .setOffsetCommitPeriodMs(10000) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
                .setMaxUncommittedOffsets(259) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
                /* processing guarantee mode is set to AT_LEAST_ONCE
                 *  that means that many methods are set to default,
                 *  eg: setEmitNullTuples(), setProcessingGuarantee(),
                 *  setTupleTrackingEnforced()
                 *  */
                .setProp()
//                .setRecordTranslator() //TODO to be set accordingly
                .build());
    }
    SpoutConfig spoutConfig = new SpoutConfig();
}

