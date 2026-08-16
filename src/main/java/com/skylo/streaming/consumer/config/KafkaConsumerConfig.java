package com.skylo.streaming.consumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Value("${ordering.mode:PER_KEY}")
    private String orderingMode;

    @Value("${ordering.topic-partitions:6}")
    private int partitions;

    @Value("${ordering.topic-name:pipeline-messages}")
    private String topicName;

    @Bean
    public NewTopic mainTopic() {
        int partitionCount = "STRICT_GLOBAL".equalsIgnoreCase(orderingMode) ? 1 : partitions;
        log.info("Creating main topic: {} with {} partitions (mode: {})", topicName, partitionCount, orderingMode);
        return TopicBuilder.name(topicName)
                .partitions(partitionCount)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic dltTopic() {
        int partitionCount = "STRICT_GLOBAL".equalsIgnoreCase(orderingMode) ? 1 : partitions;
        String dltName = topicName + ".DLT";
        log.info("Creating DLT topic: {} with {} partitions", dltName, partitionCount);
        return TopicBuilder.name(dltName)
                .partitions(partitionCount)
                .replicas(1)
                .build();
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            CommonErrorHandler commonErrorHandler) {
        
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        int concurrency = "STRICT_GLOBAL".equalsIgnoreCase(orderingMode) ? 1 : partitions;
        log.info("Setting listener concurrency to: {}", concurrency);
        factory.setConcurrency(concurrency);

        // We use manual offsets to ensure we commit only after persistence is complete
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setCommonErrorHandler(commonErrorHandler);

        return factory;
    }
}
