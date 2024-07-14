package pl.boono.kafkarebalancing;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

class Consumer {
    private final KafkaConsumer<String, Long> consumer;
    private final String topic;
    private final MessageConsumer messageConsumer;

    private volatile boolean running;

    Consumer(String bootstrapServer, String topic, String groupId, MessageConsumer messageConsumer) {
        this.consumer = new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000000
        ), new StringDeserializer(), new LongDeserializer());
        this.topic = topic;
        this.messageConsumer = messageConsumer;
    }

    void start() {
        this.running = true;
        this.consumer.subscribe(List.of(this.topic));

        new Thread(this::doRun).start();
    }

    void stop() {
        this.running = false;
    }

    private void doRun() {
        while(this.running) {
            var records = consumer.poll(Duration.ofMillis(100));
            records.forEach(this.messageConsumer);
        }
        this.consumer.unsubscribe();
    }

    @FunctionalInterface
    public interface MessageConsumer extends java.util.function.Consumer<ConsumerRecord<String, Long>> {
    }
}
