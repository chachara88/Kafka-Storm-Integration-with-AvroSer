package org.stormexample;
import com.variacode.cep.storm.esper.EsperBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;


public class simplestormTopology {
    private static final Logger LOG = LoggerFactory.getLogger(simplestormTopology.class);
//    public StormTopology build(){
//        TODO to have all teh functionality here instead of main ! This way main to be a bit cleaner
//    }

//    public StormTopology submit2LocalCluster(){
//  TODO to have all the functionality here instead of main ! This way main to be a bit cleaner
//    }
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);

        TopologyBuilder topologyBuilder = new TopologyBuilder();

//        LOG.info("AVROKAFKASPOUT: Creating AvroKafkaSpout for aeroloop_SensorReadingScalar topic\n");
//        AvroKafkaSpout SensorReadingKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("eagle5.di.uoa.gr:9092", "aeroloop_SensorReadingScalar")
//                .setOffsetCommitPeriodMs(10000) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
//                .setMaxUncommittedOffsets(259)
//                .build());
//        LOG.info("SET_TOPOLOGY_SPOUT: Setting SensorReadingSpout to the topology\n");
//        topologyBuilder.setSpout("SensorReadingSpout",SensorReadingKafkaSpout/*, 2*/); //TODO increase num of parallelism?
        LOG.info("AVROKAFKASPOUT: Creating AvroKafkaSpout for aeroloop_Voltage topic\n");
        AvroKafkaSpout VoltageKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("eagle5.di.uoa.gr:9092", "aeroloop_Voltage")
                .setOffsetCommitPeriodMs(10000)
                .setMaxUncommittedOffsets(259)
                .build());
        LOG.info("SET_TOPOLOGY_SPOUT: Setting VoltageKafkaSpout to the topology\n");
        topologyBuilder.setSpout("VoltageSpout",VoltageKafkaSpout/*, 2*/); //TODO increase num of parallelism?
        LOG.info("SET_TOPOLOGY_SPOUT: Setting tester Bolt to the topology\n");
        topologyBuilder.setBolt("testerBolt", new testerEsperBolt(), 2)
                .shuffleGrouping("VoltageSpout");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LogAnalyserStorm", config, topologyBuilder.createTopology()); //TODO to change topologyName
        Thread.sleep(1000000000);

        //Stop the topology
        cluster.shutdown();
    }
}

