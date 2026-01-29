package com.pinncode.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

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
    @Bean(name = "listerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> listerContainerFactory () {
        ConcurrentKafkaListenerContainerFactory<String, String> listenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        listenerContainerFactory.setConsumerFactory(consumerFactory());
        listenerContainerFactory.setBatchListener(true);
        return listenerContainerFactory;
    }

    /**
     * Crea mapa con propiedades para conexión kafka.
     * @return Map<String, Object> Mapa de propiedades kafka.
     */
    public Map<String, Object> producerProperties () {

        Map<String, Object> props=new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return props;
    }

    @Bean
    public KafkaTemplate<String, String> createTemplate() {
        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProperties());
        KafkaTemplate kafkaTemplate = new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }
}
