package com.pinncode.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
public class KafkaServiceApplication {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    private static final Logger log = LoggerFactory.getLogger(KafkaServiceApplication.class);

    @KafkaListener(id = "pinncodeId", autoStartup = "true",
            topics = "pinncode-topic", containerFactory = "listerContainerFactory", groupId = "pinncode-group",
    properties = {
            "max.poll.interval.ms:4000", "max.poll.records:50"
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

    @Scheduled(fixedDelay = 2000, initialDelay = 500)
    public void print () {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("pinncode-topic", String.valueOf(i), String.format("Este es un nuevo mensaje %d", i));
        }
    }

    @Scheduled(fixedDelay = 2000, initialDelay = 1000)
    public void printMetrics () {
        double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
        log.info("Count {}", count);
    }
}
