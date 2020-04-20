//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.stormexample.Spouts;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.kafka.spout.internal.*;
import org.apache.storm.kafka.spout.metrics.KafkaOffsetMetric;
import org.apache.storm.kafka.spout.subscription.TopicAssigner;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stormexample.Spouts.AvroKafkaSpoutConfig.ProcessingGuarantee;

public class AvroKafkaSpout<K, V> extends BaseRichSpout {
    private static final long serialVersionUID = 4151921085047987154L;
    public static final long TIMER_DELAY_MS = 500L;
    private static final Logger LOG = LoggerFactory.getLogger(AvroKafkaSpout.class);
    protected SpoutOutputCollector collector;
    private final AvroKafkaSpoutConfig<K, V> avroKafkaSpoutConfig;
    private final ConsumerFactory<K, V> kafkaConsumerFactory;
    private final TopicAssigner topicAssigner;
    private transient Consumer<K, V> consumer;
    private transient FirstPollOffsetStrategy firstPollOffsetStrategy;
    private transient KafkaSpoutRetryService retryService;
    private transient KafkaTupleListener tupleListener;
    private transient Timer commitTimer;
    private transient Map<TopicPartition, OffsetManager> offsetManagers;
    private transient Set<KafkaSpoutMessageId> emitted;
    private transient Map<TopicPartition, List<ConsumerRecord<K, V>>> waitingToEmit;
    private transient Timer refreshAssignmentTimer;
    private transient TopologyContext context;
    private transient AvroCommitMetadataManager commitMetadataManager;
    private transient KafkaOffsetMetric<K, V> kafkaOffsetMetric;
    private transient AvroKafkaSpout<K, V>.KafkaSpoutConsumerRebalanceListener rebalanceListener;

    public AvroKafkaSpout(AvroKafkaSpoutConfig<K, V> avroKafkaSpoutConfig) {
        this(avroKafkaSpoutConfig, new ConsumerFactoryDefault(), new TopicAssigner());
    }

    @VisibleForTesting
    AvroKafkaSpout(AvroKafkaSpoutConfig<K, V> avroKafkaSpoutConfig, ConsumerFactory<K, V> kafkaConsumerFactory, TopicAssigner topicAssigner) {
        this.kafkaConsumerFactory = kafkaConsumerFactory;
        this.topicAssigner = topicAssigner;
        this.avroKafkaSpoutConfig = avroKafkaSpoutConfig;
    }

    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
        this.firstPollOffsetStrategy = this.avroKafkaSpoutConfig.getFirstPollOffsetStrategy();
        this.retryService = this.avroKafkaSpoutConfig.getRetryService();
        this.tupleListener = this.avroKafkaSpoutConfig.getTupleListener();
        if (this.avroKafkaSpoutConfig.getProcessingGuarantee() != ProcessingGuarantee.AT_MOST_ONCE) {
            this.commitTimer = new Timer(500L, this.avroKafkaSpoutConfig.getOffsetsCommitPeriodMs(), TimeUnit.MILLISECONDS);
        }

        this.refreshAssignmentTimer = new Timer(500L, this.avroKafkaSpoutConfig.getPartitionRefreshPeriodMs(), TimeUnit.MILLISECONDS);
        this.offsetManagers = new HashMap();
        this.emitted = new HashSet();
        this.waitingToEmit = new HashMap();
        this.commitMetadataManager = new AvroCommitMetadataManager(context, this.avroKafkaSpoutConfig.getProcessingGuarantee());
        this.rebalanceListener = new KafkaSpoutConsumerRebalanceListener();
        this.consumer = this.kafkaConsumerFactory.createConsumer(this.avroKafkaSpoutConfig.getKafkaProps());
        this.tupleListener.open(conf, context);
        if (this.canRegisterMetrics()) {
            this.registerMetric();
        }

        LOG.info("Kafka Spout opened with the following configuration: {}", this.avroKafkaSpoutConfig);
    }

    private void registerMetric() {
        LOG.info("Registering Spout Metrics");
        this.kafkaOffsetMetric = new KafkaOffsetMetric(() -> {
            return Collections.unmodifiableMap(this.offsetManagers);
        }, () -> {
            return this.consumer;
        });
        this.context.registerMetric("kafkaOffset", this.kafkaOffsetMetric, this.avroKafkaSpoutConfig.getMetricsTimeBucketSizeInSecs());
    }

    private boolean canRegisterMetrics() {
        try {
            KafkaConsumer.class.getDeclaredMethod("beginningOffsets", Collection.class);
            return true;
        } catch (NoSuchMethodException var2) {
            LOG.warn("Minimum required kafka-clients library version to enable metrics is 0.10.1.0. Disabling spout metrics.");
            return false;
        }
    }

    private boolean isAtLeastOnceProcessing() {
        return this.avroKafkaSpoutConfig.getProcessingGuarantee() == ProcessingGuarantee.AT_LEAST_ONCE;
    }

    public void nextTuple() {
        try {
            if (this.refreshAssignmentTimer.isExpiredResetOnTrue()) {
                this.refreshAssignment();
            }

            if (this.commitTimer != null && this.commitTimer.isExpiredResetOnTrue()) {
                if (this.isAtLeastOnceProcessing()) {
                    this.commitOffsetsForAckedTuples();
                } else if (this.avroKafkaSpoutConfig.getProcessingGuarantee() == ProcessingGuarantee.NO_GUARANTEE) {
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = this.createFetchedOffsetsMetadata(this.consumer.assignment());
                    this.consumer.commitAsync(offsetsToCommit, (OffsetCommitCallback)null);
                    LOG.debug("Committed offsets {} to Kafka", offsetsToCommit);
                }
            }

            PollablePartitionsInfo pollablePartitionsInfo = this.getPollablePartitionsInfo();
            if (pollablePartitionsInfo.shouldPoll()) {
                try {
                    this.setWaitingToEmit(this.pollKafkaBroker(pollablePartitionsInfo));
                } catch (RetriableException var3) {
                    LOG.error("Failed to poll from kafka.", var3);
                }
            }

            this.emitIfWaitingNotEmitted();
        } catch (InterruptException var4) {
            this.throwKafkaConsumerInterruptedException();
        }

    }

    private void throwKafkaConsumerInterruptedException() {
        throw new RuntimeException(new InterruptedException("Kafka consumer was interrupted"));
    }

    private AvroKafkaSpout.PollablePartitionsInfo getPollablePartitionsInfo() {
        if (this.isWaitingToEmit()) {
            LOG.debug("Not polling. Tuples waiting to be emitted.");
            return new AvroKafkaSpout.PollablePartitionsInfo(Collections.emptySet(), Collections.emptyMap());
        } else {
            Set<TopicPartition> assignment = this.consumer.assignment();
            if (!this.isAtLeastOnceProcessing()) {
                return new AvroKafkaSpout.PollablePartitionsInfo(assignment, Collections.emptyMap());
            } else {
                Map<TopicPartition, Long> earliestRetriableOffsets = this.retryService.earliestRetriableOffsets();
                Set<TopicPartition> pollablePartitions = new HashSet();
                int maxUncommittedOffsets = this.avroKafkaSpoutConfig.getMaxUncommittedOffsets();
                Iterator var5 = assignment.iterator();

                while(true) {
                    while(var5.hasNext()) {
                        TopicPartition tp = (TopicPartition)var5.next();
                        OffsetManager offsetManager = (OffsetManager)this.offsetManagers.get(tp);
                        int numUncommittedOffsets = offsetManager.getNumUncommittedOffsets();
                        if (numUncommittedOffsets < maxUncommittedOffsets) {
                            pollablePartitions.add(tp);
                        } else {
                            long offsetAtLimit = offsetManager.getNthUncommittedOffsetAfterCommittedOffset(maxUncommittedOffsets);
                            Long earliestRetriableOffset = (Long)earliestRetriableOffsets.get(tp);
                            if (earliestRetriableOffset != null && earliestRetriableOffset <= offsetAtLimit) {
                                pollablePartitions.add(tp);
                            } else {
                                LOG.debug("Not polling on partition [{}]. It has [{}] uncommitted offsets, which exceeds the limit of [{}]. ", new Object[]{tp, numUncommittedOffsets, maxUncommittedOffsets});
                            }
                        }
                    }

                    return new AvroKafkaSpout.PollablePartitionsInfo(pollablePartitions, earliestRetriableOffsets);
                }
            }
        }
    }

    private boolean isWaitingToEmit() {
        return this.waitingToEmit.values().stream().anyMatch((list) -> {
            return !list.isEmpty();
        });
    }

    private void setWaitingToEmit(ConsumerRecords<K, V> consumerRecords) {
        Iterator var2 = consumerRecords.partitions().iterator();

        while(var2.hasNext()) {
            TopicPartition tp = (TopicPartition)var2.next();
            this.waitingToEmit.put(tp, new LinkedList(consumerRecords.records(tp)));
        }

    }

    private ConsumerRecords<K, V> pollKafkaBroker(AvroKafkaSpout.PollablePartitionsInfo pollablePartitionsInfo) {
        this.doSeekRetriableTopicPartitions(pollablePartitionsInfo.pollableEarliestRetriableOffsets);
        Set<TopicPartition> pausedPartitions = new HashSet(this.consumer.assignment());
        Set var10001 = pollablePartitionsInfo.pollablePartitions;
        pausedPartitions.removeIf(var10001::contains);

        ConsumerRecords var9;
        try {
            this.consumer.pause(pausedPartitions);
            ConsumerRecords<K, V> consumerRecords = this.consumer.poll(this.avroKafkaSpoutConfig.getPollTimeoutMs());
            this.ackRetriableOffsetsIfCompactedAway(pollablePartitionsInfo.pollableEarliestRetriableOffsets, consumerRecords);
            int numPolledRecords = consumerRecords.count();
            LOG.debug("Polled [{}] records from Kafka", numPolledRecords);
            if (this.avroKafkaSpoutConfig.getProcessingGuarantee() == ProcessingGuarantee.AT_MOST_ONCE) {
                Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = this.createFetchedOffsetsMetadata(this.consumer.assignment());
                this.consumer.commitSync(offsetsToCommit);
                LOG.debug("Committed offsets {} to Kafka", offsetsToCommit);
            }

            var9 = consumerRecords;
        } finally {
            this.consumer.resume(pausedPartitions);
        }

        return var9;
    }

    private void doSeekRetriableTopicPartitions(Map<TopicPartition, Long> pollableEarliestRetriableOffsets) {
        Iterator var2 = pollableEarliestRetriableOffsets.entrySet().iterator();

        while(var2.hasNext()) {
            Entry<TopicPartition, Long> retriableTopicPartitionAndOffset = (Entry)var2.next();
            this.consumer.seek((TopicPartition)retriableTopicPartitionAndOffset.getKey(), (Long)retriableTopicPartitionAndOffset.getValue());
        }

    }

    private void ackRetriableOffsetsIfCompactedAway(Map<TopicPartition, Long> earliestRetriableOffsets, ConsumerRecords<K, V> consumerRecords) {
        Iterator var3 = earliestRetriableOffsets.entrySet().iterator();

        while(true) {
            TopicPartition tp;
            long seekOffset;
            long earliestReceivedOffset;
            do {
                Entry entry;
                List records;
                do {
                    if (!var3.hasNext()) {
                        return;
                    }

                    entry = (Entry)var3.next();
                    tp = (TopicPartition)entry.getKey();
                    records = consumerRecords.records(tp);
                } while(records.isEmpty());

                ConsumerRecord<K, V> record = (ConsumerRecord)records.get(0);
                seekOffset = (Long)entry.getValue();
                earliestReceivedOffset = record.offset();
            } while(seekOffset >= earliestReceivedOffset);

            for(long i = seekOffset; i < earliestReceivedOffset; ++i) {
                KafkaSpoutMessageId msgId = this.retryService.getMessageId(tp, i);
                if (!((OffsetManager)this.offsetManagers.get(tp)).contains(msgId) && !this.emitted.contains(msgId)) {
                    LOG.debug("Record at offset [{}] appears to have been compacted away from topic [{}], marking as acked", i, tp);
                    this.retryService.remove(msgId);
                    this.emitted.add(msgId);
                    this.ack(msgId);
                }
            }
        }
    }

    private void emitIfWaitingNotEmitted() {
        Iterator waitingToEmitIter = this.waitingToEmit.values().iterator();

        while(waitingToEmitIter.hasNext()) {
            List waitingToEmitForTp = (List)waitingToEmitIter.next();

            while(!waitingToEmitForTp.isEmpty()) {
                boolean emittedTuple = this.emitOrRetryTuple((ConsumerRecord)waitingToEmitForTp.remove(0));
                if (emittedTuple) {
                    return;
                }
            }

            waitingToEmitIter.remove();
        }

    }

    private boolean emitOrRetryTuple(ConsumerRecord<K, V> record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        KafkaSpoutMessageId msgId = this.retryService.getMessageId(tp, record.offset());
        if (this.offsetManagers.containsKey(tp) && ((OffsetManager)this.offsetManagers.get(tp)).contains(msgId)) {
            LOG.trace("Tuple for record [{}] has already been acked. Skipping", record);
        } else if (this.emitted.contains(msgId)) {
            LOG.trace("Tuple for record [{}] has already been emitted. Skipping", record);
        } else {
            List<Object> tuple = this.avroKafkaSpoutConfig.getTranslator().apply(record);

            if (this.isEmitTuple(tuple)) {
                boolean isScheduled = this.retryService.isScheduled(msgId);
                if (!isScheduled || this.retryService.isReady(msgId)) {
                    String stream = tuple instanceof KafkaTuple ? ((KafkaTuple)tuple).getStream() : "default";
                    if (!this.isAtLeastOnceProcessing()) {
                        if (this.avroKafkaSpoutConfig.isTupleTrackingEnforced()) {
                            this.collector.emit(stream, tuple, msgId);
                        } else {
                            this.collector.emit(stream, tuple);
                        }
                    } else {
                        this.emitted.add(msgId);
                        ((OffsetManager)this.offsetManagers.get(tp)).addToEmitMsgs(msgId.offset());
                        if (isScheduled) {
                            this.retryService.remove(msgId);
                        }

                        this.collector.emit(stream, tuple, msgId); //TODO delete Christos

//                        this.collector.emit("TemperatureStream", new Values("TemperatureValue")/*, msgId TODO */);
//                        this.collector.emit("PressureStream", new Values("PressureValue")/*, msgId TODO*/);
                        this.tupleListener.onEmit(tuple, msgId);
                        LOG.error("ApacheStormMachine --> The stream is [{}]", stream);
                        LOG.error("ApacheStormMachine --> The Tuple is [{}]", tuple);
                        LOG.error("ApacheStormMachine --> The Emitted tuple [{}] for record [{}] with msgId [{}]", new Object[]{tuple, record, msgId});
                    }

                    return true;
                }
            } else {
                LOG.debug("Not emitting null tuple for record [{}] as defined in configuration.", record);
                if (this.isAtLeastOnceProcessing()) {
                    msgId.setNullTuple(true);
                    ((OffsetManager)this.offsetManagers.get(tp)).addToEmitMsgs(msgId.offset());
                    this.ack(msgId);
                }
            }
        }

        return false;
    }

    private boolean isEmitTuple(List<Object> tuple) {
        return tuple != null || this.avroKafkaSpoutConfig.isEmitNullTuples();
    }

    private Map<TopicPartition, OffsetAndMetadata> createFetchedOffsetsMetadata(Set<TopicPartition> assignedPartitions) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap();
        Iterator var3 = assignedPartitions.iterator();

        while(var3.hasNext()) {
            TopicPartition tp = (TopicPartition)var3.next();
            offsetsToCommit.put(tp, new OffsetAndMetadata(this.consumer.position(tp), this.commitMetadataManager.getCommitMetadata()));
        }

        return offsetsToCommit;
    }

    private void commitOffsetsForAckedTuples() {
        Map<TopicPartition, OffsetAndMetadata> nextCommitOffsets = new HashMap();
        Iterator var2 = this.offsetManagers.entrySet().iterator();

        Entry tpOffset;
        while(var2.hasNext()) {
            tpOffset = (Entry)var2.next();
            OffsetAndMetadata nextCommitOffset = ((OffsetManager)tpOffset.getValue()).findNextCommitOffset(this.commitMetadataManager.getCommitMetadata());
            if (nextCommitOffset != null) {
                nextCommitOffsets.put((TopicPartition) tpOffset.getKey(), nextCommitOffset);
                //TODO maybe (TopicPartition) tpOffset.getKey() casting is error
            }
        }

        if (!nextCommitOffsets.isEmpty()) {
            this.consumer.commitSync(nextCommitOffsets);
            LOG.debug("Offsets successfully committed to Kafka [{}]", nextCommitOffsets);
            var2 = nextCommitOffsets.entrySet().iterator();

            while(var2.hasNext()) {
                tpOffset = (Entry)var2.next();
                TopicPartition tp = (TopicPartition)tpOffset.getKey();
                long position = this.consumer.position(tp);
                long committedOffset = ((OffsetAndMetadata)tpOffset.getValue()).offset();
                if (position < committedOffset) {
                    LOG.debug("Consumer fell behind committed offset. Catching up. Position was [{}], skipping to [{}]", position, committedOffset);
                    this.consumer.seek(tp, committedOffset);
                }

                List<ConsumerRecord<K, V>> waitingToEmitForTp = (List)this.waitingToEmit.get(tp);
                if (waitingToEmitForTp != null) {
                    this.waitingToEmit.put(tp, waitingToEmitForTp.stream().filter((record) -> {
                        return record.offset() >= committedOffset;
                    }).collect(Collectors.toCollection(LinkedList::new)));
                }

                OffsetManager offsetManager = (OffsetManager)this.offsetManagers.get(tp);
                offsetManager.commit((OffsetAndMetadata)tpOffset.getValue());
                LOG.debug("[{}] uncommitted offsets for partition [{}] after commit", offsetManager.getNumUncommittedOffsets(), tp);
            }
        } else {
            LOG.trace("No offsets to commit. {}", this);
        }

    }

    public void ack(Object messageId) {
        if (this.isAtLeastOnceProcessing()) {
            KafkaSpoutMessageId msgId = (KafkaSpoutMessageId)messageId;
            if (msgId.isNullTuple()) {
                ((OffsetManager)this.offsetManagers.get(msgId.getTopicPartition())).addToAckMsgs(msgId);
                LOG.debug("Received direct ack for message [{}], associated with null tuple", msgId);
                this.tupleListener.onAck(msgId);
            } else {
                if (!this.emitted.contains(msgId)) {
                    LOG.debug("Received ack for message [{}], associated with tuple emitted for a ConsumerRecord that came from a topic-partition that this consumer group instance is no longer tracking due to rebalance/partition reassignment. No action taken.", msgId);
                } else {
                    Validate.isTrue(!this.retryService.isScheduled(msgId), "The message id " + msgId + " is queued for retry while being acked. This should never occur barring errors in the RetryService implementation or the spout code.");
                    ((OffsetManager)this.offsetManagers.get(msgId.getTopicPartition())).addToAckMsgs(msgId);
                    this.emitted.remove(msgId);
                }

                this.tupleListener.onAck(msgId);
            }
        }
    }

    public void fail(Object messageId) {
        if (this.isAtLeastOnceProcessing()) {
            KafkaSpoutMessageId msgId = (KafkaSpoutMessageId)messageId;
            if (!this.emitted.contains(msgId)) {
                LOG.debug("Received fail for tuple this spout is no longer tracking. Partitions may have been reassigned. Ignoring message [{}]", msgId);
            } else {
                Validate.isTrue(!this.retryService.isScheduled(msgId), "The message id " + msgId + " is queued for retry while being failed. This should never occur barring errors in the RetryService implementation or the spout code.");
                msgId.incrementNumFails();
                if (!this.retryService.schedule(msgId)) {
                    LOG.debug("Reached maximum number of retries. Message [{}] being marked as acked.", msgId);
                    this.tupleListener.onMaxRetryReached(msgId);
                    this.ack(msgId);
                } else {
                    this.tupleListener.onRetry(msgId);
                    this.emitted.remove(msgId);
                }

            }
        }
    }

    public void activate() {
        try {
            this.refreshAssignment();
        } catch (InterruptException var2) {
            this.throwKafkaConsumerInterruptedException();
        }

    }

    private void refreshAssignment() {
        Set<TopicPartition> allPartitions = this.avroKafkaSpoutConfig.getTopicFilter().getAllSubscribedPartitions(this.consumer);
        List<TopicPartition> allPartitionsSorted = new ArrayList(allPartitions);
        Collections.sort(allPartitionsSorted, TopicPartitionComparator.INSTANCE);
        Set<TopicPartition> assignedPartitions = this.avroKafkaSpoutConfig.getTopicPartitioner().getPartitionsForThisTask(allPartitionsSorted, this.context);
        this.topicAssigner.assignPartitions(this.consumer, assignedPartitions, this.rebalanceListener);
    }

    public void deactivate() {
        try {
            this.commitIfNecessary();
        } catch (InterruptException var2) {
            this.throwKafkaConsumerInterruptedException();
        }

    }

    public void close() {
        try {
            this.shutdown();
        } catch (InterruptException var2) {
            this.throwKafkaConsumerInterruptedException();
        }

    }

    private void commitIfNecessary() {
        if (this.isAtLeastOnceProcessing()) {
            this.commitOffsetsForAckedTuples();
        }

    }

    private void shutdown() {
        try {
            this.commitIfNecessary();
        } finally {
            this.consumer.close();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        RecordTranslator<K, V> translator = this.avroKafkaSpoutConfig.getTranslator();
        Iterator var3 = translator.streams().iterator();
        while(var3.hasNext()) {
            String stream = (String)var3.next();
            declarer.declareStream(stream, translator.getFieldsFor(stream));
        }

    }

    public String toString() {
        return "KafkaSpout{offsetManagers =" + this.offsetManagers + ", emitted=" + this.emitted + "}";
    }

    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> configuration = super.getComponentConfiguration();
        if (configuration == null) {
            configuration = new HashMap();
        }

        String configKeyPrefix = "config.";
        ((Map)configuration).put(configKeyPrefix + "topics", this.getTopicsString());
        ((Map)configuration).put(configKeyPrefix + "groupid", this.avroKafkaSpoutConfig.getConsumerGroupId());
        Iterator var3 = this.avroKafkaSpoutConfig.getKafkaProps().entrySet().iterator();

        while(true) {
            while(var3.hasNext()) {
                Entry<String, Object> conf = (Entry)var3.next();
                if (conf.getValue() != null && this.isPrimitiveOrWrapper(conf.getValue().getClass())) {
                    ((Map)configuration).put(configKeyPrefix + (String)conf.getKey(), conf.getValue());
                } else {
                    LOG.debug("Dropping Kafka prop '{}' from component configuration", conf.getKey());
                }
            }

            return (Map)configuration;
        }
    }

    private boolean isPrimitiveOrWrapper(Class<?> type) {
        if (type == null) {
            return false;
        } else {
            return type.isPrimitive() || this.isWrapper(type);
        }
    }

    private boolean isWrapper(Class<?> type) {
        return type == Double.class || type == Float.class || type == Long.class || type == Integer.class || type == Short.class || type == Character.class || type == Byte.class || type == Boolean.class || type == String.class;
    }

    private String getTopicsString() {
        return this.avroKafkaSpoutConfig.getTopicFilter().getTopicsString();
    }

    @VisibleForTesting
    KafkaOffsetMetric<K, V> getKafkaOffsetMetric() {
        return this.kafkaOffsetMetric;
    }

    private static class PollablePartitionsInfo {
        private final Set<TopicPartition> pollablePartitions;
        private final Map<TopicPartition, Long> pollableEarliestRetriableOffsets;

        public PollablePartitionsInfo(Set<TopicPartition> pollablePartitions, Map<TopicPartition, Long> earliestRetriableOffsets) {
            this.pollablePartitions = pollablePartitions;
            this.pollableEarliestRetriableOffsets = (Map)earliestRetriableOffsets.entrySet().stream().filter((entry) -> {
                return pollablePartitions.contains(entry.getKey());
            }).collect(Collectors.toMap((entry) -> {
                return (TopicPartition)entry.getKey();
            }, (entry) -> {
                return (Long)entry.getValue();
            }));
        }

        public boolean shouldPoll() {
            return !this.pollablePartitions.isEmpty();
        }
    }

    private class KafkaSpoutConsumerRebalanceListener implements ConsumerRebalanceListener {
        private Collection<TopicPartition> previousAssignment;

        private KafkaSpoutConsumerRebalanceListener() {
            this.previousAssignment = new HashSet();
        }

        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            this.previousAssignment = partitions;
            AvroKafkaSpout.LOG.info("Partitions revoked. [consumer-group={}, consumer={}, topic-partitions={}]", new Object[]{AvroKafkaSpout.this.avroKafkaSpoutConfig.getConsumerGroupId(), AvroKafkaSpout.this.consumer, partitions});
            if (AvroKafkaSpout.this.isAtLeastOnceProcessing()) {
                AvroKafkaSpout.this.commitOffsetsForAckedTuples();
            }

        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            AvroKafkaSpout.LOG.info("Partitions reassignment. [task-ID={}, consumer-group={}, consumer={}, topic-partitions={}]", new Object[]{AvroKafkaSpout.this.context.getThisTaskId(), AvroKafkaSpout.this.avroKafkaSpoutConfig.getConsumerGroupId(), AvroKafkaSpout.this.consumer, partitions});
            this.initialize(partitions);
            AvroKafkaSpout.this.tupleListener.onPartitionsReassigned(partitions);
        }

        private void initialize(Collection<TopicPartition> partitions) {
            if (AvroKafkaSpout.this.isAtLeastOnceProcessing()) {
                AvroKafkaSpout.this.offsetManagers.keySet().retainAll(partitions);
                AvroKafkaSpout.this.retryService.retainAll(partitions);
                AvroKafkaSpout.this.emitted.removeIf((msgId) -> {
                    return !partitions.contains(msgId.getTopicPartition());
                });
            }

            AvroKafkaSpout.this.waitingToEmit.keySet().retainAll(partitions);
            Set<TopicPartition> newPartitions = new HashSet(partitions);
            newPartitions.removeAll(this.previousAssignment);
            Iterator var3 = newPartitions.iterator();

            while(var3.hasNext()) {
                TopicPartition newTp = (TopicPartition)var3.next();
                OffsetAndMetadata committedOffset = AvroKafkaSpout.this.consumer.committed(newTp);
                long fetchOffset = this.doSeek(newTp, committedOffset);
                AvroKafkaSpout.LOG.debug("Set consumer position to [{}] for topic-partition [{}] with [{}] and committed offset [{}]", new Object[]{fetchOffset, newTp, AvroKafkaSpout.this.firstPollOffsetStrategy, committedOffset});
                if (AvroKafkaSpout.this.isAtLeastOnceProcessing() && !AvroKafkaSpout.this.offsetManagers.containsKey(newTp)) {
                    AvroKafkaSpout.this.offsetManagers.put(newTp, new OffsetManager(newTp, fetchOffset));
                }
            }

            AvroKafkaSpout.LOG.info("Initialization complete");
        }

        private long doSeek(TopicPartition newTp, OffsetAndMetadata committedOffset) {
            AvroKafkaSpout.LOG.trace("Seeking offset for topic-partition [{}] with [{}] and committed offset [{}]", new Object[]{newTp, AvroKafkaSpout.this.firstPollOffsetStrategy, committedOffset});
            if (committedOffset != null) {
                if (AvroKafkaSpout.this.commitMetadataManager.isOffsetCommittedByThisTopology(newTp, committedOffset, Collections.unmodifiableMap(AvroKafkaSpout.this.offsetManagers))) {
                    AvroKafkaSpout.this.consumer.seek(newTp, committedOffset.offset());
                } else if (AvroKafkaSpout.this.firstPollOffsetStrategy.equals(FirstPollOffsetStrategy.EARLIEST)) {
                    AvroKafkaSpout.this.consumer.seekToBeginning(Collections.singleton(newTp));
                } else if (AvroKafkaSpout.this.firstPollOffsetStrategy.equals(FirstPollOffsetStrategy.LATEST)) {
                    AvroKafkaSpout.this.consumer.seekToEnd(Collections.singleton(newTp));
                } else {
                    AvroKafkaSpout.this.consumer.seek(newTp, committedOffset.offset());
                }
            } else if (!AvroKafkaSpout.this.firstPollOffsetStrategy.equals(FirstPollOffsetStrategy.EARLIEST) && !AvroKafkaSpout.this.firstPollOffsetStrategy.equals(FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST)) {
                if (AvroKafkaSpout.this.firstPollOffsetStrategy.equals(FirstPollOffsetStrategy.LATEST) || AvroKafkaSpout.this.firstPollOffsetStrategy.equals(FirstPollOffsetStrategy.UNCOMMITTED_LATEST)) {
                    AvroKafkaSpout.this.consumer.seekToEnd(Collections.singleton(newTp));
                }
            } else {
                AvroKafkaSpout.this.consumer.seekToBeginning(Collections.singleton(newTp));
            }

            return AvroKafkaSpout.this.consumer.position(newTp);
        }
    }
}
