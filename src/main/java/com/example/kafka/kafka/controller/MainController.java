package com.example.kafka.kafka.controller;

import com.example.kafka.kafka.service.producer.ProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/publish")
public class MainController {

    private final ProducerService producerService;

    @Autowired
    public MainController(ProducerService producerService) {
        this.producerService = producerService;
    }

    @GetMapping(value = "/start")
    public void start() {
        producerService.run();
    }

}
