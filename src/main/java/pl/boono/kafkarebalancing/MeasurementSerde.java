package pl.boono.kafkarebalancing;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

import static pl.boono.kafkarebalancing.ConsumerProducerRunner.objectMapper;

public class MeasurementSerde implements Serde<Measurement> {
    @Override
    public Serializer<Measurement> serializer() {
        return (ignoredTopic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        };
    }

    @Override
    public Deserializer<Measurement> deserializer() {
        return (ignoredTopic, data) -> {
            try {
                return objectMapper.readValue(data, Measurement.class);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
