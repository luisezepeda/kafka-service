package com.pinncode.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
public class KafkaServiceApplication implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger log = LoggerFactory.getLogger(KafkaServiceApplication.class);

    @KafkaListener(id = "pinncodeId", autoStartup = "false",
            topics = "pinncode-topic", containerFactory = "listerContainerFactory", groupId = "pinncode-group",
    properties = {
            "max.poll.interval.ms:4000", "max.poll.records:10"
    })
    public void listen (List<ConsumerRecord<String, String>> messages) {
        for(ConsumerRecord<String, String> message : messages) {
            log.info("Partition = {}, Offset = {}, Key = {}, Value = {}", message.partition(), message.offset(), message.key(), message.value());
        }
        log.info("Batch complete");
    }

	public static void main(String[] args) {
		SpringApplication.run(KafkaServiceApplication.class, args);
	}

    @Scheduled(fixedDelay = 1000, initialDelay = 500)
    public void print () {
        log.info("Pinncode rocks");
    }

    @Override
    public void run(String... args) throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("pinncode-topic", String.valueOf(i), String.format("Este es un nuevo mensaje %d", i));
        }
    }
}
