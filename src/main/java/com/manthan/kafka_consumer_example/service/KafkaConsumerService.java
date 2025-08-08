package com.manthan.kafka_consumer_example.service;

import com.manthan.kafka_consumer_example.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
    @KafkaListener(topics = "manthan-topic", groupId = "manthan-group")
    public void consume(User user) {
        System.out.println("✅ [CONSUMER] Received user: " + user.getName() + ", age: " + user.getAge());
    }
}

