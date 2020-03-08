//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.stormexample;

import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.annotation.InterfaceStability.Unstable;
import org.apache.storm.kafka.spout.EmptyKafkaTupleListener;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff.TimeInterval;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.kafka.spout.KafkaTupleListener;
import org.apache.storm.kafka.spout.internal.CommonKafkaSpoutConfig;
import org.apache.storm.kafka.spout.subscription.ManualPartitioner;
import org.apache.storm.kafka.spout.subscription.TopicFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroKafkaSpoutConfig<K, V> extends CommonKafkaSpoutConfig<K, V> {
    private static final long serialVersionUID = 141902646130682494L;
    public static final long DEFAULT_OFFSET_COMMIT_PERIOD_MS = 30000L;
    public static final int DEFAULT_MAX_RETRIES = 2147483647;
    public static final int DEFAULT_MAX_UNCOMMITTED_OFFSETS = 10000000;
    public static final KafkaSpoutRetryService DEFAULT_RETRY_SERVICE = new KafkaSpoutRetryExponentialBackoff(TimeInterval.seconds(0L), TimeInterval.milliSeconds(2L), 2147483647, TimeInterval.seconds(10L));
    public static final AvroKafkaSpoutConfig.ProcessingGuarantee DEFAULT_PROCESSING_GUARANTEE;
    public static final KafkaTupleListener DEFAULT_TUPLE_LISTENER;
    public static final Logger LOG;
    public static final int DEFAULT_METRICS_TIME_BUCKET_SIZE_SECONDS = 60;
    private final long offsetCommitPeriodMs;
    private final int maxUncommittedOffsets;
    private final KafkaSpoutRetryService retryService;
    private final KafkaTupleListener tupleListener;
    private final boolean emitNullTuples;
    private final AvroKafkaSpoutConfig.ProcessingGuarantee processingGuarantee;
    private final boolean tupleTrackingEnforced;
    private final int metricsTimeBucketSizeInSecs;

    public AvroKafkaSpoutConfig(AvroKafkaSpoutConfig.Builder<K, V> builder) {
        super(builder.setKafkaPropsForProcessingGuarantee());
        this.offsetCommitPeriodMs = builder.offsetCommitPeriodMs;
        this.maxUncommittedOffsets = builder.maxUncommittedOffsets;
        this.retryService = builder.retryService;
        this.tupleListener = builder.tupleListener;
        this.emitNullTuples = builder.emitNullTuples;
        this.processingGuarantee = builder.processingGuarantee;
        this.tupleTrackingEnforced = builder.tupleTrackingEnforced;
        this.metricsTimeBucketSizeInSecs = builder.metricsTimeBucketSizeInSecs;
    }

    public static AvroKafkaSpoutConfig.Builder<String, String> builder(String bootstrapServers, String... topics) {
        return (new AvroKafkaSpoutConfig.Builder(bootstrapServers, topics)).withCustomAvroDeserializer();
    }

    public static AvroKafkaSpoutConfig.Builder<String, String> builder(String bootstrapServers, Set<String> topics) {
        return (new AvroKafkaSpoutConfig.Builder(bootstrapServers, topics)).withCustomAvroDeserializer();
    }

    public static AvroKafkaSpoutConfig.Builder<String, String> builder(String bootstrapServers, Pattern topics) {
        return (new AvroKafkaSpoutConfig.Builder(bootstrapServers, topics)).withCustomAvroDeserializer();
    }

    public long getOffsetsCommitPeriodMs() {
        return this.offsetCommitPeriodMs;
    }

    public AvroKafkaSpoutConfig.ProcessingGuarantee getProcessingGuarantee() {
        return this.processingGuarantee;
    }

    public boolean isTupleTrackingEnforced() {
        return this.tupleTrackingEnforced;
    }

    public String getConsumerGroupId() {
        return (String)this.getKafkaProps().get("group.id");
    }

    public int getMaxUncommittedOffsets() {
        return this.maxUncommittedOffsets;
    }

    public KafkaSpoutRetryService getRetryService() {
        return this.retryService;
    }

    public KafkaTupleListener getTupleListener() {
        return this.tupleListener;
    }

    public boolean isEmitNullTuples() {
        return this.emitNullTuples;
    }

    public int getMetricsTimeBucketSizeInSecs() {
        return this.metricsTimeBucketSizeInSecs;
    }

    public String toString() {
        return (new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)).append("offsetCommitPeriodMs", this.offsetCommitPeriodMs).append("maxUncommittedOffsets", this.maxUncommittedOffsets).append("retryService", this.retryService).append("tupleListener", this.tupleListener).append("processingGuarantee", this.processingGuarantee).append("emitNullTuples", this.emitNullTuples).append("tupleTrackingEnforced", this.tupleTrackingEnforced).append("metricsTimeBucketSizeInSecs", this.metricsTimeBucketSizeInSecs).toString();
    }

    static {
        DEFAULT_PROCESSING_GUARANTEE = AvroKafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE;
        DEFAULT_TUPLE_LISTENER = new EmptyKafkaTupleListener();
        LOG = LoggerFactory.getLogger(AvroKafkaSpoutConfig.class);
    }

    public static class Builder<K, V> extends org.apache.storm.kafka.spout.internal.CommonKafkaSpoutConfig.Builder<K, V, AvroKafkaSpoutConfig.Builder<K, V>> {
        private long offsetCommitPeriodMs = 30000L;
        private int maxUncommittedOffsets = 10000000;
        private KafkaSpoutRetryService retryService;
        private KafkaTupleListener tupleListener;
        private boolean emitNullTuples;
        private AvroKafkaSpoutConfig.ProcessingGuarantee processingGuarantee;
        private boolean tupleTrackingEnforced;
        private int metricsTimeBucketSizeInSecs;

        public Builder(String bootstrapServers, String... topics) {
            super(bootstrapServers, topics);
            this.retryService = AvroKafkaSpoutConfig.DEFAULT_RETRY_SERVICE;
            this.tupleListener = AvroKafkaSpoutConfig.DEFAULT_TUPLE_LISTENER;
            this.emitNullTuples = false;
            this.processingGuarantee = AvroKafkaSpoutConfig.DEFAULT_PROCESSING_GUARANTEE;
            this.tupleTrackingEnforced = false;
            this.metricsTimeBucketSizeInSecs = 60;
        }

        public Builder(String bootstrapServers, Set<String> topics) {
            super(bootstrapServers, topics);
            this.retryService = AvroKafkaSpoutConfig.DEFAULT_RETRY_SERVICE;
            this.tupleListener = AvroKafkaSpoutConfig.DEFAULT_TUPLE_LISTENER;
            this.emitNullTuples = false;
            this.processingGuarantee = AvroKafkaSpoutConfig.DEFAULT_PROCESSING_GUARANTEE;
            this.tupleTrackingEnforced = false;
            this.metricsTimeBucketSizeInSecs = 60;
        }

        public Builder(String bootstrapServers, Pattern topics) {
            super(bootstrapServers, topics);
            this.retryService = AvroKafkaSpoutConfig.DEFAULT_RETRY_SERVICE;
            this.tupleListener = AvroKafkaSpoutConfig.DEFAULT_TUPLE_LISTENER;
            this.emitNullTuples = false;
            this.processingGuarantee = AvroKafkaSpoutConfig.DEFAULT_PROCESSING_GUARANTEE;
            this.tupleTrackingEnforced = false;
            this.metricsTimeBucketSizeInSecs = 60;
        }

        public Builder(String bootstrapServers, TopicFilter topicFilter, ManualPartitioner topicPartitioner) {
            super(bootstrapServers, topicFilter, topicPartitioner);
            this.retryService = AvroKafkaSpoutConfig.DEFAULT_RETRY_SERVICE;
            this.tupleListener = AvroKafkaSpoutConfig.DEFAULT_TUPLE_LISTENER;
            this.emitNullTuples = false;
            this.processingGuarantee = AvroKafkaSpoutConfig.DEFAULT_PROCESSING_GUARANTEE;
            this.tupleTrackingEnforced = false;
            this.metricsTimeBucketSizeInSecs = 60;
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
            this.offsetCommitPeriodMs = offsetCommitPeriodMs;
            return this;
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setMaxUncommittedOffsets(int maxUncommittedOffsets) {
            this.maxUncommittedOffsets = maxUncommittedOffsets;
            return this;
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setRetry(KafkaSpoutRetryService retryService) {
            if (retryService == null) {
                throw new NullPointerException("retryService cannot be null");
            } else {
                this.retryService = retryService;
                return this;
            }
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setTupleListener(KafkaTupleListener tupleListener) {
            if (tupleListener == null) {
                throw new NullPointerException("KafkaTupleListener cannot be null");
            } else {
                this.tupleListener = tupleListener;
                return this;
            }
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setEmitNullTuples(boolean emitNullTuples) {
            this.emitNullTuples = emitNullTuples;
            return this;
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setProcessingGuarantee(AvroKafkaSpoutConfig.ProcessingGuarantee processingGuarantee) {
            this.processingGuarantee = processingGuarantee;
            return this;
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setTupleTrackingEnforced(boolean tupleTrackingEnforced) {
            this.tupleTrackingEnforced = tupleTrackingEnforced;
            return this;
        }

        public AvroKafkaSpoutConfig.Builder<K, V> setMetricsTimeBucketSizeInSecs(int metricsTimeBucketSizeInSecs) {
            this.metricsTimeBucketSizeInSecs = metricsTimeBucketSizeInSecs;
            return this;
        }

        private AvroKafkaSpoutConfig.Builder<K, V> withStringDeserializers() {
            this.setProp("key.deserializer", StringDeserializer.class);
            this.setProp("value.deserializer", StringDeserializer.class);
            return this;
        }

        private AvroKafkaSpoutConfig.Builder<K, V> withCustomAvroDeserializer() {
            this.setProp("key.deserializer", CustomAvroDeserializer.class);
            this.setProp("value.deserializer", CustomAvroDeserializer.class);
            return this;
        }

        private AvroKafkaSpoutConfig.Builder<K, V> setKafkaPropsForProcessingGuarantee() {
            if (this.getKafkaProps().containsKey("enable.auto.commit")) {
                throw new IllegalStateException("The KafkaConsumer enable.auto.commit setting is not supported. You can configure similar behavior through KafkaSpoutConfig.Builder.setProcessingGuarantee");
            } else {
                String autoOffsetResetPolicy = (String)this.getKafkaProps().get("auto.offset.reset");
                if (this.processingGuarantee == AvroKafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE) {
                    if (autoOffsetResetPolicy == null) {
                        AvroKafkaSpoutConfig.LOG.info("Setting Kafka consumer property '{}' to 'earliest' to ensure at-least-once processing", "auto.offset.reset");
                        this.setProp("auto.offset.reset", "earliest");
                    } else if (!autoOffsetResetPolicy.equals("earliest") && !autoOffsetResetPolicy.equals("none")) {
                        AvroKafkaSpoutConfig.LOG.warn("Cannot guarantee at-least-once processing with auto.offset.reset.policy other than 'earliest' or 'none'. Some messages may be skipped.");
                    }
                } else if (this.processingGuarantee == AvroKafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE && autoOffsetResetPolicy != null && !autoOffsetResetPolicy.equals("latest") && !autoOffsetResetPolicy.equals("none")) {
                    AvroKafkaSpoutConfig.LOG.warn("Cannot guarantee at-most-once processing with auto.offset.reset.policy other than 'latest' or 'none'. Some messages may be processed more than once.");
                }

                AvroKafkaSpoutConfig.LOG.info("Setting Kafka consumer property '{}' to 'false', because the spout does not support auto-commit", "enable.auto.commit");
                this.setProp("enable.auto.commit", false);
                return this;
            }
        }

        public AvroKafkaSpoutConfig<K, V> build() {
            return new AvroKafkaSpoutConfig(this);
        }
    }

    @Unstable
    public static enum ProcessingGuarantee {
        AT_LEAST_ONCE,
        AT_MOST_ONCE,
        NO_GUARANTEE;

        private ProcessingGuarantee() {
        }
    }
}
