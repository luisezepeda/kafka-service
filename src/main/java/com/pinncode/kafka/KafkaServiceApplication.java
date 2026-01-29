package com.pinncode.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

@SpringBootApplication
public class KafkaServiceApplication implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(KafkaServiceApplication.class);

    @KafkaListener(topics = "pinncode-topic", containerFactory = "listerContainerFactory", groupId = "pinncode-group",
    properties = {
            "max.poll.interval.ms:4000", "max.poll.records:10"
    })
    public void listen (List<String> messages) {
        for(String message : messages) {
            log.info("Mensaje recibido :: {}", message);
        }
        log.info("Batch complete");
    }

	public static void main(String[] args) {
		SpringApplication.run(KafkaServiceApplication.class, args);
	}

    @Override
    public void run(String... args) {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("pinncode-topic", String.format("Este es un nuevo mensaje %d", i));
        }
    }
}
