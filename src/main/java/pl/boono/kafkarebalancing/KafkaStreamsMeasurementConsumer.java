package pl.boono.kafkarebalancing;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.kafka.KafkaStreamsMetrics;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.util.List;
import java.util.Map;
import java.util.Properties;

class KafkaStreamsMeasurementConsumer implements MeasurementConsumer {
    private final KafkaStreams streams;
    private final KafkaStreamsMetrics metrics;
    private MessageConsumer messageConsumer;

    KafkaStreamsMeasurementConsumer(String bootstrapServer,
                                    String topic,
                                    String groupId,
                                    Map<String, Object> additionalProperties,
                                    int id) {
        var properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1000000);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MeasurementSerde.class);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.putAll(additionalProperties);

        var builder = new StreamsBuilder();
        builder.<String, Measurement>stream(topic).processValues(MeasurementProcessor::new);
        this.streams = new KafkaStreams(builder.build(), properties);
        this.metrics = new KafkaStreamsMetrics(this.streams, List.of(Tag.of("consumer_id", String.valueOf(id))));
        this.metrics.bindTo(Metrics.globalRegistry);
    }

    @Override
    public void setRecordConsumer(MessageConsumer consumer) {
        this.messageConsumer = consumer;
    }

    @Override
    public void start() {
        this.streams.start();
    }

    @Override
    public void stop() {
        this.metrics.close();
        this.streams.close();
    }

    @RequiredArgsConstructor
    private class MeasurementProcessor implements FixedKeyProcessor<String, Measurement, Measurement> {
        private FixedKeyProcessorContext<String, Measurement> context;

        @Override
        public void init(FixedKeyProcessorContext<String, Measurement> context) {
            this.context = context;
        }

        @Override
        public void process(FixedKeyRecord<String, Measurement> record) {
            this.context.recordMetadata().ifPresent(metadata -> {
                messageConsumer.accept(new ConsumedRecord<>(metadata.partition(), record.value()));
            });
        }
    }
}
