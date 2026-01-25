package com.pinncode.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración Kafka
 */
@Configuration
public class KafkaConfiguration {

    /**
     * Crea mapa con propiedades para conexión kafka.
     * @return Map<String, Object> Mapa de propiedades kafka.
     */
    public Map<String, Object> consumerProperties () {

        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "pinncode-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return properties;
    }

    /**
     *
     * @return
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory () {
        return new DefaultKafkaConsumerFactory<>(consumerProperties());
    }

    /**
     *
     * @return
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> listerContainerFactory () {
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory());
        return listenerContainerFactory;
    }
}
