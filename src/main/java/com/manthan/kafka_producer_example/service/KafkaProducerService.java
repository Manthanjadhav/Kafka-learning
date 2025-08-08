package com.manthan.kafka_producer_example.service;

import com.manthan.kafka_producer_example.model.User;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, User> kafkaTemplate;
    private static final String TOPIC = "kafka-topic";

    public KafkaProducerService(KafkaTemplate<String, User> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendUser(User user) {
        for(int i=0;i<10000;i++)
        {
            user.setAge(i);
            kafkaTemplate.send(TOPIC, user);
        }
    }
}

