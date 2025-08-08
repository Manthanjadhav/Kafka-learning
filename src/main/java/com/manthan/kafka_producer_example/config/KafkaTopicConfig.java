package com.manthan.kafka_producer_example.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic createManthanTopic() {
        return new NewTopic("kafka-topic", 5, (short) 1);
    }
}
