package org.stormexample;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.Bolt;
import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

        //Creating SPouts
        LOG.info("ApacheStormMachine --> Creating AvroKafkaSpout for aeroloop_SensorReadingScalar topic\n");
        AvroKafkaSpout SensorReadingKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("eagle5.di.uoa.gr:9092", "aeroloop_SensorReadingScalar")
                .setOffsetCommitPeriodMs(10000) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
                .setMaxUncommittedOffsets(259)
                .build());

        LOG.error("ApacheStormMachine --> Creating AvroKafkaSpout for aeroloop_Voltage topic\n");
        AvroKafkaSpout VoltageKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("eagle5.di.uoa.gr:9092", "aeroloop_Voltage")
                .setOffsetCommitPeriodMs(10000)
                .setMaxUncommittedOffsets(259)
                .build());

//        Setting Spouts to the topology
        LOG.info("ApacheStormMachine --> Setting SensorReadingSpout to the topology\n");
        topologyBuilder.setSpout("SensorReadingSpout",SensorReadingKafkaSpout/*, 2*/); //TODO increase num of parallelism?

        LOG.info("ApacheStormMachine --> Setting VoltageKafkaSpout to the topology\n");
        topologyBuilder.setSpout("VoltageSpout",VoltageKafkaSpout/*, 2*/); //TODO increase num of parallelism?

        //Setting the Bolts into the topology
        LOG.info("ApacheStormMachine --> Setting tester Bolt to the topology\n");
        topologyBuilder.setBolt("simpleVoltageBolt", new SimpleVoltageBolt()/*, 2*/)
                .shuffleGrouping("VoltageSpout");

        topologyBuilder.setBolt("SimpleEsperVoltageValueBolt", new MonitorValueBolt()/*,2*/)
                 .shuffleGrouping("simpleVoltageBolt");


        LOG.info("ApacheStormMachine --> Setting sourceModuleExtractor Bolt to the topology\n");
        topologyBuilder.setBolt("sourceModuleExtractor", new SourceModuleExtractor(), 2)
                .shuffleGrouping("SensorReadingSpout");

        //Has to be set up in next Esper Bolt
        //                .fieldsGrouping("TemperatureStream", new Fields("TemperatureValue"))
        //                .fieldsGrouping("PressureStream", new Fields("PressureValue"));


        // Create MongoMapper
        LOG.info("ApacheStormMachine --> Creating Voltage and Sensor Mongo Mappers \n");
        MongoMapper voltageMapper = new SimpleMongoMapper();
               // .withFields("topic","partition","offset","key","value"); //TODO Remove Comments
        MongoMapper sensorMapper = new SimpleMongoMapper();
               // .withFields("topic","partition","offset","key","value"); //TODO Remove Comments

        LOG.info("ApacheStormMachine --> Setting MongoDataPersistence Bolts to the topology\n");
        LOG.info("ApacheStormMachine --> MongoDBVoltageDataPersistence Bolt Details are:" + "\nURL:" + " mongodb://127.0.0.1:27017/aeroloopVolt" + "\nCollectionName:" + "VoltageTuples\n" );
        topologyBuilder.setBolt("MongoDBVoltageDataPersistence", new MongoInsertBolt( "mongodb://127.0.0.1:27017/aeroloopVolt"," VoltageTuples", voltageMapper))
                .shuffleGrouping("VoltageSpout");
        LOG.info("ApacheStormMachine --> MongoDBSensorDataPersistence Bolt Details are:" + "\nURL:" + " mongodb://127.0.0.1:27017/aeroloopSens" + "\nCollectionName:" + "SensorTuples\n" );
        topologyBuilder.setBolt("MongoDBSensorDataPersistence", new MongoInsertBolt("mongodb://127.0.0.1:27017/aeroloopSens", " SensorTuples", sensorMapper))
                .shuffleGrouping("SensorReadingSpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Esper-Storm_with_MONGODB_Persistence_Topology", config, topologyBuilder.createTopology()); //TODO to change topologyName
//        Thread.sleep(300000);
        Utils.sleep(600000);

        //Stop the topology
        cluster.shutdown();
    }
}

