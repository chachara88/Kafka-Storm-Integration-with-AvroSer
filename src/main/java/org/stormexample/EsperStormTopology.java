package org.stormexample;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;

import org.apache.storm.mongodb.bolt.MongoInsertBolt;
import org.apache.storm.mongodb.common.mapper.MongoMapper;
import org.apache.storm.mongodb.common.mapper.SimpleMongoMapper;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.Bolts.*;
import org.stormexample.Spouts.AvroKafkaSpout;
import org.stormexample.Spouts.AvroKafkaSpoutConfig;


public class EsperStormTopology {
    private static final Logger LOG = LoggerFactory.getLogger(EsperStormTopology.class);
    public enum  Query {
        AVERAGE,
        WARNING,
        CRITICAL,
        JOIN
    }
    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setDebug(true);
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //Creating SPouts
        LOG.info("ApacheStormMachine --> Creating AvroKafkaSpout for aeroloop_SensorReadingScalar topic\n");
        AvroKafkaSpout SensorReadingKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("IP_ADDRESS:PORT_NUMBER", "TOPIC_NAME")
                .setOffsetCommitPeriodMs(10000) // KafkaSpoutConfig.ProcessingGuarantee is KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE.
                .setMaxUncommittedOffsets(259)
                .build());

        LOG.info("ApacheStormMachine --> Creating AvroKafkaSpout for aeroloop_Voltage topic\n");
        AvroKafkaSpout VoltageKafkaSpout = new AvroKafkaSpout(AvroKafkaSpoutConfig.builder("IP_ADDRESS:PORT_NUMBER", "TOPIC_NAME")
                .setOffsetCommitPeriodMs(10000)
                .setMaxUncommittedOffsets(259)
                .build());

        //Setting Spouts to the topology
        LOG.info("ApacheStormMachine --> Setting SensorReadingSpout to the topology\n");
        topologyBuilder.setSpout("SensorReadingSpout",SensorReadingKafkaSpout/*, 2*/); //TODO increase num of parallelism?

        LOG.info("ApacheStormMachine --> Setting VoltageKafkaSpout to the topology\n");
        topologyBuilder.setSpout("VoltageSpout",VoltageKafkaSpout/*, 2*/); //TODO increase num of parallelism?

        //Setting the Bolts into the topology
        LOG.info("ApacheStormMachine --> Setting ValueExtractorBolt Bolt to the topology\n");
        topologyBuilder.setBolt("simpleVoltageBolt", new ValueExtractorBolt()/*, 2*/)
                .shuffleGrouping("VoltageSpout");
        LOG.info("ApacheStormMachine --> Setting VoltageBolt Bolt to calculate the average values\n");
        topologyBuilder.setBolt("AverageVoltage_Bolt", new VoltageEsperBolt(Query.AVERAGE)/*, 2*/)
                .shuffleGrouping("simpleVoltageBolt");

        LOG.info("ApacheStormMachine --> Setting VoltageEsperBolt Bolt to the topology\n");
        topologyBuilder.setBolt("WarningVoltage_Bolt", new VoltageEsperBolt(Query.WARNING)/*, 2*/)
                .shuffleGrouping("simpleVoltageBolt");
        LOG.info("ApacheStormMachine --> Setting VoltageEsperBolt Bolt to the topology\n");
        topologyBuilder.setBolt("CriticalVoltage_Bolt", new VoltageEsperBolt(Query.CRITICAL)/*, 2*/)
                .shuffleGrouping("simpleVoltageBolt");


        LOG.info("ApacheStormMachine --> Setting DefaultStreamSplitter Bolt to the topology\n");
        topologyBuilder.setBolt("defaultStreamSplitter", new DefaultStreamSplitter(), 2)
                .shuffleGrouping("SensorReadingSpout");
        LOG.info("ApacheStormMachine --> Setting ValueExtractorBolt Bolt for Temperature and Pressure streams to the topology\n");
        topologyBuilder.setBolt("TemperatureValueExtractor", new ValueExtractorBolt()).shuffleGrouping("defaultStreamSplitter", "TemperatureStream");
        topologyBuilder.setBolt("PressureValueExtractor", new ValueExtractorBolt()).shuffleGrouping("defaultStreamSplitter", "PressureStream");

        LOG.info("ApacheStormMachine --> Setting Temperature and Pressure EsperBolts" +
                " to calculate the average values \n");
        topologyBuilder.setBolt("AverageTemperature_Bolt", new TemperatureEsperBolt(Query.AVERAGE))
                .shuffleGrouping("TemperatureValueExtractor");
        topologyBuilder.setBolt("AveragePressure_Bolt", new PressureEsperBolt(Query.AVERAGE))
                .shuffleGrouping("PressureValueExtractor");

        LOG.info("ApacheStormMachine --> Setting Temperature and Pressure EsperBolts" +
                "to check for warning queries");
        topologyBuilder.setBolt("WarningTemperature_Bolt", new TemperatureEsperBolt(Query.WARNING))
                .shuffleGrouping("TemperatureValueExtractor");
        topologyBuilder.setBolt("WarningPressure_Bolt", new PressureEsperBolt(Query.WARNING))
                .shuffleGrouping("PressureValueExtractor");

        LOG.info("ApacheStormMachine --> Setting Temperature and Pressure EsperBolts" +
                "to check for critical queries");
        topologyBuilder.setBolt("CriticalTemperature_Bolt", new TemperatureEsperBolt(Query.CRITICAL))
                .shuffleGrouping("TemperatureValueExtractor");
        topologyBuilder.setBolt("CriticalPressure_Bolt", new PressureEsperBolt(Query.CRITICAL))
                .shuffleGrouping("PressureValueExtractor");

        // Create MongoMapper
        LOG.info("ApacheStormMachine --> Creating Voltage and Sensor Mongo Mappers \n");
        MongoMapper voltageMapper = new SimpleMongoMapper();
               // .withFields("topic","partition","offset","key","value");
        MongoMapper sensorMapper = new SimpleMongoMapper();
               // .withFields("topic","partition","offset","key","value");

        LOG.info("ApacheStormMachine --> Setting MongoDataPersistence Bolts to the topology\n");
        LOG.info("ApacheStormMachine --> MongoDBVoltageDataPersistence Bolt Details are:" + "\nURL:" + " mongodb://127.0.0.1:27017/aeroloopVolt" + "\nCollectionName:" + "VoltageTuples\n" );
        topologyBuilder.setBolt("MongoDBVoltageDataPersistence", new MongoInsertBolt( "mongodb://127.0.0.1:27017/aeroloopVolt"," VoltageTuples", voltageMapper))
                .shuffleGrouping("VoltageSpout");
        LOG.info("ApacheStormMachine --> MongoDBSensorDataPersistence Bolt Details are:" + "\nURL:" + " mongodb://127.0.0.1:27017/aeroloopSens" + "\nCollectionName:" + "SensorTuples\n" );
        topologyBuilder.setBolt("MongoDBSensorDataPersistence", new MongoInsertBolt("mongodb://127.0.0.1:27017/aeroloopSens", " SensorTuples", sensorMapper))
                .shuffleGrouping("SensorReadingSpout");

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Esper-Storm_with_MONGODB_Persistence_Topology", config, topologyBuilder.createTopology());
//        Thread.sleep(300000);
        Utils.sleep(600000);

        //Stop the topology
        cluster.shutdown();
    }
}

