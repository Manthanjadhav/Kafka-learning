package com.manthan.kafka_producer_example.controller;



import com.manthan.kafka_producer_example.model.User;
import com.manthan.kafka_producer_example.service.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/publish")
public class KafkaProducerController {

    private final KafkaProducerService service;

    public KafkaProducerController(KafkaProducerService service) {
        this.service = service;
    }

    @PostMapping
    public String sendUser(@RequestBody User user) {
        service.sendUser(user);
        return "Sent user: " + user.getName();
    }
}
