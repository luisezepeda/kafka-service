package com.pinncode.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class KafkaServiceApplication implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(KafkaServiceApplication.class);

    @KafkaListener(topics = "pinncode-topic", groupId = "pinncode-group")
    public void listen (String message) {
        log.info("Mensaje recibido :: {}", message);
    }

	public static void main(String[] args) {
		SpringApplication.run(KafkaServiceApplication.class, args);
	}

    @Override
    public void run(String... args) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("pinncode-topic", "Este es un nuevo mensahe");
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent message with offset=[{}] to partition [{}]", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
            } else {
                log.error("Unable to send message due to : {}", ex.getMessage());
            }
        });
    }
}
