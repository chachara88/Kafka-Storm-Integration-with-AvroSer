package org.stormexample.Spouts;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.storm.kafka.spout.internal.CommitMetadata;
import org.apache.storm.kafka.spout.internal.OffsetManager;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.Spouts.AvroKafkaSpoutConfig.ProcessingGuarantee;

public final class AvroCommitMetadataManager {
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(org.apache.storm.kafka.spout.internal.CommitMetadataManager.class);
    private final String commitMetadata;
    private final ProcessingGuarantee processingGuarantee;
    private final TopologyContext context;

    public AvroCommitMetadataManager(TopologyContext context, ProcessingGuarantee processingGuarantee) {
        this.context = context;

        try {
            this.commitMetadata = JSON_MAPPER.writeValueAsString(new CommitMetadata(context.getStormId(), context.getThisTaskId(), Thread.currentThread().getName()));
            this.processingGuarantee = processingGuarantee;
        } catch (JsonProcessingException var4) {
            LOG.error("Failed to create Kafka commit metadata due to JSON serialization error", var4);
            throw new RuntimeException(var4);
        }
    }

    public boolean isOffsetCommittedByThisTopology(TopicPartition tp, OffsetAndMetadata committedOffset, Map<TopicPartition, OffsetManager> offsetManagers) {
        try {
            if (this.processingGuarantee == ProcessingGuarantee.AT_LEAST_ONCE && offsetManagers.containsKey(tp) && ((OffsetManager)offsetManagers.get(tp)).hasCommitted()) {
                return true;
            } else {
                CommitMetadata committedMetadata = (CommitMetadata)JSON_MAPPER.readValue(committedOffset.metadata(), CommitMetadata.class);
                return committedMetadata.getTopologyId().equals(this.context.getStormId());
            }
        } catch (IOException var5) {
            LOG.warn("Failed to deserialize expected commit metadata [{}]. This error is expected to occur once per partition, if the last commit to each partition was by an earlier version of the KafkaSpout, or by a process other than the KafkaSpout. Defaulting to behavior compatible with earlier version", committedOffset);
            LOG.trace("", var5);
            return false;
        }
    }

    public String getCommitMetadata() {
        return this.commitMetadata;
    }
}
