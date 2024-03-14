package pl.boono.kafkarebalancing;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class ConsumerProducerRunner {
    public static final Meter.MeterProvider<DistributionSummary> consumerLatencySummaryBuilder = DistributionSummary
            .builder("consumer_latency")
            .maximumExpectedValue((double) TimeUnit.SECONDS.toMillis(5))
            .publishPercentileHistogram()
            .withRegistry(Metrics.globalRegistry);
    private final KafkaConfig kafkaConfig;
    private final Producer producer;
    private final Queue<Consumer> consumers = new ArrayDeque<>();

    public ConsumerProducerRunner(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
        this.producer = new Producer(
                kafkaConfig.getBootstrapServers(),
                kafkaConfig.getTopic(),
                kafkaConfig.getProducer().getBatchSize());

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    private void shutdown() {
        log.info("Stopping producer and consumers.");
        this.producer.stop();
        this.consumers.forEach(Consumer::stop);
    }

    void startProducer() {
        if (!this.producer.isRunning()) {
            this.producer.start();
        }
    }

    void stopProducer() {
        this.producer.stop();
    }

    void addConsumer(String groupId) {
        var consumer = new Consumer(
                this.kafkaConfig.getBootstrapServers(),
                this.kafkaConfig.getTopic(),
                groupId,
                record -> this.logLatency(record, this.consumers.size(), groupId));
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

    private void logLatency(ConsumerRecord<String, String> record, int consumerId, String groupId) {
        var latencyMs = Instant.now().toEpochMilli() - record.timestamp();
        consumerLatencySummaryBuilder.withTags(List.of(
                Tag.of("consumer_id", String.valueOf(consumerId)),
                Tag.of("group_id", groupId),
                Tag.of("partition_id", String.valueOf(record.partition()))
        )).record(latencyMs);
    }
}
