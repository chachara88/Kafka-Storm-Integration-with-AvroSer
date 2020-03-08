package org.stormexample;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


public class simplestormTopology {
    private static final Logger LOG = LoggerFactory.getLogger(simplestormTopology.class);
    public StormTopology build(){
        TopologyBuilder topologyBuilder = new TopologyBuilder();
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
        /*TODO Set will be removed once we conclude in topics, we will use*/
        List<AvroKafkaSpout> avroSpoutList = null;

        LOG.info("KAFKASPOUT: Configuring the KafkaSpout");
        Iterator<String> topicsSetIterator = setOfTopics.iterator();
        while (topicsSetIterator.hasNext()) { //Create the list of the spout(s) and then tehy are set in topology
            String KafkaSpoutName = topicsSetIterator.next() + "Spout";
            AvroKafkaSpout avroKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("localhost:6667", topicsSetIterator.next())
                    .setOffsetCommitPeriodMs(10000) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
                    .setMaxUncommittedOffsets(259)
                    .build());
            topologyBuilder.setSpout(KafkaSpoutName, avroKafkaSpout, 1);
        }
        return topologyBuilder.createTopology();
    }

    public static void main(String[] args) throws Exception {

    }
}

