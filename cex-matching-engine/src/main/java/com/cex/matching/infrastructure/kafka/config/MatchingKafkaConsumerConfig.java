package com.cex.matching.infrastructure.kafka.config;

import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

/** 将分区生命周期监听器接入 Spring Boot 创建的 Kafka ConsumerFactory。 */
@Configuration
@RequiredArgsConstructor
public class MatchingKafkaConsumerConfig {
    private final MatchingConsumerRebalanceListener rebalanceListener;

    /** 为撮合监听器创建带有分区生命周期保护的独立容器工厂。 */
    @Bean("matchingKafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<Object, Object> matchingKafkaListenerContainerFactory(
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(
                org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
        return factory;
    }
}
