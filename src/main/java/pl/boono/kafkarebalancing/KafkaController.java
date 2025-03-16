package pl.boono.kafkarebalancing;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/kafka")
public class KafkaController {
    private final ConsumerProducerRunner consumerProducerRunner;

    @PostMapping("/producer/start")
    ResponseEntity<?> startProducer() {
        consumerProducerRunner.startProducer();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/producer/stop")
    ResponseEntity<?> stopProducer() {
        consumerProducerRunner.stopProducer();
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/consumer/add")
    ResponseEntity<?> addConsumer(
            @RequestParam("groupId") String groupId,
            @RequestParam("instanceId") String instanceId,
            @RequestParam(value = "consumerType", defaultValue = "Kafka") ConsumerType consumerType) {
        switch (consumerType) {
            case Kafka -> consumerProducerRunner.addKafkaConsumer(groupId, Map.of(
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId
            ));
            case KafkaStreams -> consumerProducerRunner.addKafkaStreamsConsumer(groupId, Map.of(
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId
            ));
        }
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/consumer/remove")
    ResponseEntity<?> removeConsumer() {
        consumerProducerRunner.removeConsumer();
        return ResponseEntity.noContent().build();
    }

    enum ConsumerType {
        Kafka,
        KafkaStreams
    }
}
