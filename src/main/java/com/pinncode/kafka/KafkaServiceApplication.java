package com.pinncode.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;

@SpringBootApplication
public class KafkaServiceApplication {

    private static final Logger log = LoggerFactory.getLogger(KafkaServiceApplication.class);

    @KafkaListener(topics = "pinncode-topic", groupId = "pinncode-group")
    public void listen (String message) {
        log.info("Mensaje recibido :: {}", message);
    }

	public static void main(String[] args) {
		SpringApplication.run(KafkaServiceApplication.class, args);
	}
}
