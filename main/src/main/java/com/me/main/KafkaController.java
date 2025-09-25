package com.me.main;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {
    @Autowired
    private KafkaProducerService producerService;

    @PostMapping("/send")
    public ResponseEntity<String> sendMessage(@RequestParam String message) {
        producerService.sendMessage(message);
        return ResponseEntity.ok("Message sent: " + message);
    }
}