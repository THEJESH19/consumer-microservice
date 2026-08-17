package com.skylo.streaming.consumer.config;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.lang.reflect.Field;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaConsumerErrorHandlerTest {

    @Test
    void testCommonErrorHandlerConfig() {
        // Arrange
        DeadLetterPublishingRecoverer recoverer = mock(DeadLetterPublishingRecoverer.class);
        KafkaConsumerErrorHandler config = new KafkaConsumerErrorHandler(null);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "retryIntervalMs", 5000L);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "maxAttempts", 4);

        // Act
        CommonErrorHandler errorHandler = config.commonErrorHandler(recoverer);

        // Assert
        assertThat(errorHandler).isInstanceOf(DefaultErrorHandler.class);
        BackOff backOff = findBackOffField(errorHandler);
        assertThat(backOff).isNotNull();
        assertThat(backOff).isInstanceOf(FixedBackOff.class);
        
        FixedBackOff fixedBackOff = (FixedBackOff) backOff;
        assertThat(fixedBackOff.getInterval()).isEqualTo(5000L);
        assertThat(fixedBackOff.getMaxAttempts()).isEqualTo(3L); // maxAttempts - 1 (3 retries)
    }

    @Test
    void testCommonErrorHandlerConfig_MinAttempts() {
        // Arrange
        DeadLetterPublishingRecoverer recoverer = mock(DeadLetterPublishingRecoverer.class);
        KafkaConsumerErrorHandler config = new KafkaConsumerErrorHandler(null);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "retryIntervalMs", 1000L);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "maxAttempts", 1); // 1 total attempt = 0 retries

        // Act
        CommonErrorHandler errorHandler = config.commonErrorHandler(recoverer);

        // Assert
        assertThat(errorHandler).isInstanceOf(DefaultErrorHandler.class);
        BackOff backOff = findBackOffField(errorHandler);
        assertThat(backOff).isNotNull();
        assertThat(backOff).isInstanceOf(FixedBackOff.class);
        
        FixedBackOff fixedBackOff = (FixedBackOff) backOff;
        assertThat(fixedBackOff.getInterval()).isEqualTo(1000L);
        assertThat(fixedBackOff.getMaxAttempts()).isEqualTo(0L); // 0 backoff attempts
    }

    @Test
    void testCommonErrorHandlerConfig_UnlimitedAttempts() {
        // Arrange
        DeadLetterPublishingRecoverer recoverer = mock(DeadLetterPublishingRecoverer.class);
        KafkaConsumerErrorHandler config = new KafkaConsumerErrorHandler(null);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "retryIntervalMs", 2000L);
        org.springframework.test.util.ReflectionTestUtils.setField(config, "maxAttempts", -1); // <= 0 means unlimited

        // Act
        CommonErrorHandler errorHandler = config.commonErrorHandler(recoverer);

        // Assert
        assertThat(errorHandler).isInstanceOf(DefaultErrorHandler.class);
        BackOff backOff = findBackOffField(errorHandler);
        assertThat(backOff).isNotNull();
        assertThat(backOff).isInstanceOf(FixedBackOff.class);
        
        FixedBackOff fixedBackOff = (FixedBackOff) backOff;
        assertThat(fixedBackOff.getInterval()).isEqualTo(2000L);
        assertThat(fixedBackOff.getMaxAttempts()).isEqualTo(FixedBackOff.UNLIMITED_ATTEMPTS);
    }

    private BackOff findBackOffField(Object target) {
        if (target == null) {
            return null;
        }
        Class<?> current = target.getClass();
        while (current != null && current != Object.class) {
            for (Field field : current.getDeclaredFields()) {
                if (BackOff.class.isAssignableFrom(field.getType())) {
                    field.setAccessible(true);
                    try {
                        return (BackOff) field.get(target);
                    } catch (IllegalAccessException e) {
                        // ignore
                    }
                }
            }
            // Check if there is a FailedRecordTracker or similar helper field and recursively search it
            for (Field field : current.getDeclaredFields()) {
                String typeName = field.getType().getSimpleName();
                if (typeName.contains("Tracker") || typeName.contains("Processor") || typeName.contains("Handler")) {
                    field.setAccessible(true);
                    try {
                        Object subObject = field.get(target);
                        if (subObject != null && subObject != target) {
                            BackOff bo = findBackOffField(subObject);
                            if (bo != null) {
                                return bo;
                            }
                        }
                    } catch (IllegalAccessException e) {
                        // ignore
                    }
                }
            }
            current = current.getSuperclass();
        }
        return null;
    }
}
