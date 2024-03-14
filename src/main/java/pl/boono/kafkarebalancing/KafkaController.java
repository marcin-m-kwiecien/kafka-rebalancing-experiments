package pl.boono.kafkarebalancing;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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
    ResponseEntity<?> addConsumer(@RequestParam("groupId") String groupId) {
        consumerProducerRunner.addConsumer(groupId);
        return ResponseEntity.noContent().build();
    }

    @PostMapping("/consumer/remove")
    ResponseEntity<?> removeConsumer() {
        consumerProducerRunner.removeConsumer();
        return ResponseEntity.noContent().build();
    }

}
