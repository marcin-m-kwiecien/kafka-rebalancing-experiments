package pl.boono.kafkarebalancing;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class ConsumerProducerRunner {
    public static final Meter.MeterProvider<DistributionSummary> consumerLatencySummaryBuilder = DistributionSummary
            .builder("consumer_latency_ns_ktk")
            .minimumExpectedValue((double) TimeUnit.MICROSECONDS.toNanos(100))
            .maximumExpectedValue((double) TimeUnit.SECONDS.toNanos(10))
            .publishPercentileHistogram()
            .withRegistry(Metrics.globalRegistry);
    public static final ObjectMapper objectMapper = new ObjectMapper()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .registerModule(new JavaTimeModule());
    private final KafkaConfig kafkaConfig;
    private final Producer producer;
    private final Queue<MeasurementConsumer> consumers = new ArrayDeque<>();

    public ConsumerProducerRunner(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.producer = new Producer(
                kafkaConfig.getBootstrapServers(),
                kafkaConfig.getTopic(),
                kafkaConfig.getProducer().getBatchSize(),
                objectMapper);

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    private void shutdown() {
        log.info("Stopping producer and consumers.");
        this.producer.stop();
        this.consumers.forEach(MeasurementConsumer::stop);
    }

    void startProducer() {
        if (!this.producer.isRunning()) {
            this.producer.start();
        }
    }

    void stopProducer() {
        this.producer.stop();
    }

    void addKafkaConsumer(String groupId, Map<String, Object> additionalProperties) {
        var consumerId = this.consumers.size();
        var consumer = new KafkaMeasurementConsumer(
                this.kafkaConfig.getBootstrapServers(),
                this.kafkaConfig.getTopic(),
                groupId,
                objectMapper,
                additionalProperties,
                consumerId);
        consumer.setRecordConsumer(record -> this.logLatency(record, consumerId, groupId));
        this.consumers.offer(consumer);
        consumer.start();
    }

    void addKafkaStreamsConsumer(String groupId, Map<String, Object> additionalProperties) {
        int consumerId = this.consumers.size();
        var consumer = new KafkaStreamsMeasurementConsumer(
                this.kafkaConfig.getBootstrapServers(),
                this.kafkaConfig.getTopic(),
                groupId,
                additionalProperties,
                consumerId);
        consumer.setRecordConsumer(record -> this.logLatency(record, consumerId, groupId));
        this.consumers.offer(consumer);
        consumer.start();
    }

    void removeConsumer() {
        if (this.consumers.isEmpty()) {
            return;
        }

        var consumerToStop = this.consumers.remove();
        consumerToStop.stop();
    }

    private void logLatency(ConsumedRecord<Measurement> record, int consumerId, String groupId) {
        var latency = Duration.between(record.value().cloudTimestamp(), Instant.now());
        if (latency.minus(Duration.ofSeconds(10)).isPositive()) {
            log.warn("Received record with very high latency ({}) at partition {}.", latency, record.partition());
        }
        consumerLatencySummaryBuilder.withTags(List.of(
                Tag.of("consumer_id", String.valueOf(consumerId)),
                Tag.of("group_id", groupId),
                Tag.of("partition_id", String.valueOf(record.partition()))
        )).record(latency.toNanos());
    }
}
