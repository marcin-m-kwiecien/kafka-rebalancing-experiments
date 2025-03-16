package pl.boono.kafkarebalancing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class KafkaMeasurementConsumer implements MeasurementConsumer {
    private final KafkaConsumer<String, Measurement> consumer;
    private final KafkaClientMetrics metrics;
    private final String topic;
    private MessageConsumer messageConsumer = __ -> {};

    private volatile boolean running;

    KafkaMeasurementConsumer(String bootstrapServer,
                             String topic,
                             String groupId,
                             ObjectMapper objectMapper,
                             Map<String, Object> additionalProperties,
                             int id) {
        var properties = new HashMap<String, Object>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest",
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000000
        ));
        properties.putAll(additionalProperties);
        this.consumer = new KafkaConsumer<>(properties, new StringDeserializer(), (__, data) -> {
            try {
                return objectMapper.readValue(data, Measurement.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        this.topic = topic;
        this.metrics = new KafkaClientMetrics(this.consumer, List.of(Tag.of("consumer_id", String.valueOf(id))));
        this.metrics.bindTo(Metrics.globalRegistry);
    }

    @Override
    public void setRecordConsumer(MessageConsumer consumer) {
        this.messageConsumer = consumer;
    }

    @Override
    public void start() {
        this.running = true;
        this.consumer.subscribe(List.of(this.topic));

        new Thread(this::doRun).start();
    }

    @Override
    public void stop() {
        this.running = false;
    }

    private void doRun() {
        while(this.running) {
            var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> this.messageConsumer.accept(
                    new ConsumedRecord<>(record.partition(), record.value())));
        }
        this.consumer.unsubscribe();
    }
}
