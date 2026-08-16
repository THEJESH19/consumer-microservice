package com.skylo.streaming.consumer.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skylo.streaming.consumer.client.MessageDeliveryClient;
import com.skylo.streaming.consumer.dto.MessageDto;
import com.skylo.streaming.consumer.exception.NonRetryableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PipelineMessageListener {

    private final MessageDeliveryClient deliveryClient;
    private final ObjectMapper objectMapper;

    private static final String MDC_MESSAGE_ID = "messageId";
    private static final String MDC_MESSAGE_KEY = "messageKey";
    private static final String MDC_RETRY_COUNT = "retryCount";

    @KafkaListener(
            topics = "${ordering.topic-name:pipeline-messages}",
            groupId = "pipeline-consumer-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onMessage(
            ConsumerRecord<String, String> record,
            Acknowledgment acknowledgment,
            @Header(name = KafkaHeaders.DELIVERY_ATTEMPT, required = false) Integer deliveryAttempt) {
        
        int attempt = (deliveryAttempt != null) ? deliveryAttempt : 1;
        
        MDC.put(MDC_MESSAGE_KEY, record.key());
        MDC.put(MDC_RETRY_COUNT, String.valueOf(attempt - 1));

        try {
            log.info("Processing message from partition {}, offset {} (key: {}, attempt: {})",
                    record.partition(), record.offset(), record.key(), attempt);

            MessageDto message;
            try {
                message = objectMapper.readValue(record.value(), MessageDto.class);
            } catch (JsonProcessingException e) {
                log.error("Failed to parse JSON payload on partition {}, offset {}: {}",
                        record.partition(), record.offset(), e.getMessage());
                throw new NonRetryableException("Malformed message payload", e);
            }

            if (message != null && message.getId() != null) {
                MDC.put(MDC_MESSAGE_ID, message.getId());
            }

            deliveryClient.deliver(message);

            // Acknowledge the offset only after successful processing/persistance downstream
            acknowledgment.acknowledge();

        } finally {
            MDC.clear();
        }
    }
}
