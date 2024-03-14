package pl.boono.kafkarebalancing;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;

@Component
@RequiredArgsConstructor
public class TopicCreator implements CommandLineRunner {
    private final KafkaConfig kafkaConfig;

    @Override
    public void run(String... args) throws Exception {
        var topicName = this.kafkaConfig.getTopic();
        try (var admin = Admin.create(Map.of(BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers()))) {
            var topics = admin.listTopics().names().get();
            if (topics.contains(topicName)) {
                return;
            }

            var newTopic = new NewTopic(
                    topicName,
                    this.kafkaConfig.getTopicPartitions(),
                    (short) 1);
            admin.createTopics(List.of(newTopic)).all().get();
        }
    }
}
