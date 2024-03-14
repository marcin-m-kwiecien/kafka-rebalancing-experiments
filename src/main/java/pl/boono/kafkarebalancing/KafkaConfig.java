package pl.boono.kafkarebalancing;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "kafka")
public class KafkaConfig {
    private String bootstrapServers;
    private String topic;
    private int topicPartitions;
    private KafkaProducerConfig producer;


    @Data
    public static class KafkaProducerConfig {
        private int batchSize;
    }
}
