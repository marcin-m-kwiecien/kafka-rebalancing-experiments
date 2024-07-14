package pl.boono.kafkarebalancing;

import io.micrometer.core.instrument.Metrics;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

class Producer {
    private final KafkaProducer<String, Long> producer;
    private final String topic;
    private final int batchSize;

    @Getter
    private volatile boolean running;

    Producer(String bootstrapServers, String topic, int batchSize) {
        this.producer = new KafkaProducer<>(
                Map.of(
                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                        ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip",
                        ProducerConfig.LINGER_MS_CONFIG, "100"
                ),
                new StringSerializer(),
                new LongSerializer());
        this.topic = topic;
        this.batchSize = batchSize;
    }

    void start() {
        this.running = true;
        new Thread(this::doRun).start();
    }

    void stop() {
        this.running = false;
    }

    private void doRun() {
        var sentWithoutFlush = 0;
        while (this.running) {
            var nanos = System.nanoTime();
            var record = new ProducerRecord<>(this.topic, null,
                    Instant.now().toEpochMilli(), UUID.randomUUID().toString(), nanos);
            producer.send(record);
            sentWithoutFlush++;
            if (sentWithoutFlush >= this.batchSize) {
                Metrics.counter("sent_in_batch").increment(sentWithoutFlush);
                producer.flush();
                sentWithoutFlush = 0;
            }
        }
        producer.close(Duration.ofSeconds(10));
    }
}
