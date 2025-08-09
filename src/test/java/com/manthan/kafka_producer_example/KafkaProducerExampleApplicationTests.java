package com.manthan.kafka_producer_example;

import com.manthan.kafka_producer_example.model.User;
import com.manthan.kafka_producer_example.service.KafkaProducerService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.Duration;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(SpringExtension.class)
@SpringBootTest
class KafkaProducerExampleApplicationTests {

	@Autowired
	private KafkaProducerService producerService;

	private static final String TOPIC = "kafka-topic-new-1";
	private static final String DOCKER_KAFKA_BOOTSTRAP = "localhost:9092"; // adjust if Docker uses another host/port

	@Test
	void testProducerSends100UsersToDockerKafka() {
		// given
		User baseUser = new User("Manthan", 0);

		// when
		producerService.sendUser(baseUser);

		// then
		Map<String, Object> consumerProps = new HashMap<>();
		consumerProps.put("bootstrap.servers", DOCKER_KAFKA_BOOTSTRAP);
		consumerProps.put("group.id", "testGroup-" + UUID.randomUUID()); // unique group ID per test
		consumerProps.put("auto.offset.reset", "earliest");
		consumerProps.put("enable.auto.commit", "false");
		consumerProps.put("key.deserializer", StringDeserializer.class.getName());
		consumerProps.put("value.deserializer", JsonDeserializer.class.getName());
		consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, User.class.getName());
		consumerProps.put("spring.json.trusted.packages", "*");

		List<User> receivedUsers = new ArrayList<>();
		try (KafkaConsumer<String, User> consumer = new KafkaConsumer<>(consumerProps)) {
			consumer.subscribe(Collections.singletonList(TOPIC));

			long endTime = System.currentTimeMillis() + 10000; // wait up to 10 seconds
			while (System.currentTimeMillis() < endTime && receivedUsers.size() < 100) {
				ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(200));
				for (ConsumerRecord<String, User> record : records) {
					receivedUsers.add(record.value());
				}
			}
		}

		assertThat(receivedUsers)
				.as("We should receive exactly 100 User objects")
				.hasSize(100);

		assertThat(receivedUsers.get(0).getName()).isEqualTo("Manthan");
	}
}
