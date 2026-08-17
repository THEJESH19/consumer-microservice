package com.skylo.streaming.consumer.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.skylo.streaming.consumer.client.MessageDeliveryClient;
import com.skylo.streaming.consumer.dto.MessageDto;
import com.skylo.streaming.consumer.exception.NonRetryableException;
import com.skylo.streaming.consumer.exception.RetryableException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@SpringBootTest
@Testcontainers
class PipelineMessageListenerIT {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.4.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        // Set short retry interval in testing to run tests fast
        registry.add("retry.interval-ms", () -> "500");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private MessageDeliveryClient deliveryClient;

    @Autowired
    private ConsumerFactory<String, String> consumerFactory;

    private Consumer<String, String> testDltConsumer;

    @BeforeEach
    void setUp() {
        // Prepare consumer for DLT topic verification
        Map<String, Object> consumerProps = new HashMap<>(KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "test-dlt-group", "true"));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        DefaultKafkaConsumerFactory<String, String> dltConsumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        testDltConsumer = dltConsumerFactory.createConsumer();
        testDltConsumer.subscribe(Collections.singletonList("pipeline-messages.DLT"));
    }

    @AfterEach
    void tearDown() {
        if (testDltConsumer != null) {
            testDltConsumer.close();
        }
    }

    @Test
    void testMessageConsumption_SuccessPath() throws Exception {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        MessageDto message = MessageDto.builder()
                .id(messageId)
                .key("device-12")
                .payload("Happy path integration test")
                .timestamp(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(message);

        // Act
        kafkaTemplate.send("pipeline-messages", "device-12", payload).get();

        // Assert - Verify delivery client is invoked
        verify(deliveryClient, timeout(10000)).deliver(any(MessageDto.class));
    }

    @Test
    void testMessageConsumption_RetryThenSucceed() throws Exception {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        MessageDto message = MessageDto.builder()
                .id(messageId)
                .key("device-13")
                .payload("Retry integration test")
                .timestamp(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(message);

        // Mock delivery client to fail first, then succeed
        Mockito.doThrow(new RetryableException("Simulated connection refused"))
               .doNothing()
               .when(deliveryClient).deliver(any(MessageDto.class));

        // Act
        kafkaTemplate.send("pipeline-messages", "device-13", payload).get();

        // Assert - Verify delivery client is invoked twice (once failed, then success)
        verify(deliveryClient, timeout(10000).times(2)).deliver(any(MessageDto.class));
    }

    @Test
    void testMessageConsumption_NonRetryableSendsToDlt() throws Exception {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        MessageDto message = MessageDto.builder()
                .id(messageId)
                .key("device-14")
                .payload("DLT integration test")
                .timestamp(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(message);

        // Mock delivery client to throw NonRetryableException
        Mockito.doThrow(new NonRetryableException("Simulated HTTP 400 Bad Request"))
               .when(deliveryClient).deliver(any(MessageDto.class));

        // Act
        kafkaTemplate.send("pipeline-messages", "device-14", payload).get();

        // Assert - Verify DLT consumption receives the record
        ConsumerRecords<String, String> records = testDltConsumer.poll(Duration.ofSeconds(15));
        
        assertThat(records.count()).isGreaterThanOrEqualTo(1);
        ConsumerRecord<String, String> dltRecord = records.iterator().next();
        assertThat(dltRecord.key()).isEqualTo("device-14");
        assertThat(dltRecord.value()).contains(messageId);
    }

    @Test
    void testMessageConsumption_RetryExhaustionSendsToDlt() throws Exception {
        // Arrange
        String messageId = UUID.randomUUID().toString();
        MessageDto message = MessageDto.builder()
                .id(messageId)
                .key("device-15")
                .payload("Retry exhaustion test")
                .timestamp(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(message);

        // Mock delivery client to fail repeatedly with a RetryableException
        Mockito.doThrow(new RetryableException("Simulated downstream outage"))
               .when(deliveryClient).deliver(any(MessageDto.class));

        // Act
        kafkaTemplate.send("pipeline-messages", "device-15", payload).get();

        // Assert - Verify delivery client is invoked exactly 3 times (1 initial + 2 retries)
        verify(deliveryClient, timeout(10000).times(3)).deliver(any(MessageDto.class));

        // Assert - Verify DLT consumption receives the record after retries are exhausted
        ConsumerRecords<String, String> records = testDltConsumer.poll(Duration.ofSeconds(15));
        
        assertThat(records.count()).isGreaterThanOrEqualTo(1);
        boolean foundMessage = false;
        for (ConsumerRecord<String, String> dltRecord : records) {
            if ("device-15".equals(dltRecord.key()) && dltRecord.value().contains(messageId)) {
                foundMessage = true;
                break;
            }
        }
        assertThat(foundMessage).isTrue();
    }
}

