package com.skylo.streaming.consumer.config;

import com.skylo.streaming.consumer.exception.NonRetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerErrorHandler {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${retry.interval-ms:10000}")
    private long retryIntervalMs;

    @Value("${ordering.topic-name:pipeline-messages}")
    private String mainTopicName;

    @Bean
    public DeadLetterPublishingRecoverer dltRecoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> {
            String dltTopic = mainTopicName + ".DLT";
            log.error("Routing poisoned message (key: {}) to DLT: {} partition: {} due to: {}",
                    record.key(), dltTopic, record.partition(), exception.getMessage());
            return new TopicPartition(dltTopic, record.partition());
        });
    }

    @Bean
    public CommonErrorHandler commonErrorHandler(DeadLetterPublishingRecoverer dltRecoverer) {
        log.info("Setting up DefaultErrorHandler with {} ms retry backoff", retryIntervalMs);
        
        FixedBackOff fixedBackOff = new FixedBackOff(retryIntervalMs, FixedBackOff.UNLIMITED_ATTEMPTS);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(dltRecoverer, fixedBackOff);

        // Instantly route to DLT if we encounter a non-retryable exception
        errorHandler.addNotRetryableExceptions(NonRetryableException.class);
        
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("Delivery attempt {} failed for key: {} (partition: {}). Retrying in {} ms. Cause: {}",
                    deliveryAttempt, record.key(), record.partition(), retryIntervalMs, ex.getMessage());
        });

        return errorHandler;
    }
}
