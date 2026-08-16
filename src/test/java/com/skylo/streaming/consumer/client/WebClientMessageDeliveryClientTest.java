package com.skylo.streaming.consumer.client;

import com.skylo.streaming.consumer.client.impl.WebClientMessageDeliveryClient;
import com.skylo.streaming.consumer.dto.MessageDto;
import com.skylo.streaming.consumer.exception.NonRetryableException;
import com.skylo.streaming.consumer.exception.RetryableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("rawtypes")
class WebClientMessageDeliveryClientTest {

    @Mock
    private WebClient.Builder webClientBuilder;

    @Mock
    private WebClient webClient;

    @Mock
    private WebClient.RequestBodyUriSpec requestBodyUriSpec;

    @Mock
    private WebClient.RequestBodySpec requestBodySpec;

    @Mock
    private WebClient.RequestHeadersSpec requestHeadersSpec;

    @Mock
    private WebClient.ResponseSpec responseSpec;

    private WebClientMessageDeliveryClient client;

    @BeforeEach
    void setUp() {
        when(webClientBuilder.baseUrl(anyString())).thenReturn(webClientBuilder);
        when(webClientBuilder.build()).thenReturn(webClient);
        
        client = new WebClientMessageDeliveryClient(webClientBuilder, "http://localhost:8082");

        // Setup fluent WebClient mocks
        lenient().when(webClient.post()).thenReturn(requestBodyUriSpec);
        lenient().when(requestBodyUriSpec.uri(anyString())).thenReturn(requestBodySpec);
        lenient().when(requestBodySpec.contentType(any())).thenReturn(requestBodySpec);
        lenient().when(requestBodySpec.bodyValue(any())).thenReturn(requestHeadersSpec);
        lenient().when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
    }

    @Test
    void testDeliver_Success_201() {
        MessageDto dto = MessageDto.builder().id("1").payload("ok").timestamp(Instant.now()).build();
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.empty());

        assertDoesNotThrow(() -> client.deliver(dto));
    }

    @Test
    void testDeliver_Duplicate_208() {
        MessageDto dto = MessageDto.builder().id("1").payload("ok").timestamp(Instant.now()).build();
        WebClientResponseException ex = new WebClientResponseException(
                HttpStatus.ALREADY_REPORTED.value(), "Already Reported", null, null, null);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(ex));

        assertDoesNotThrow(() -> client.deliver(dto));
    }

    @Test
    void testDeliver_ClientError_400_ThrowsNonRetryable() {
        MessageDto dto = MessageDto.builder().id("1").payload("ok").timestamp(Instant.now()).build();
        WebClientResponseException ex = new WebClientResponseException(
                HttpStatus.BAD_REQUEST.value(), "Bad Request", null, null, null);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(ex));

        assertThrows(NonRetryableException.class, () -> client.deliver(dto));
    }

    @Test
    void testDeliver_ServerError_500_ThrowsRetryable() {
        MessageDto dto = MessageDto.builder().id("1").payload("ok").timestamp(Instant.now()).build();
        WebClientResponseException ex = new WebClientResponseException(
                HttpStatus.INTERNAL_SERVER_ERROR.value(), "Internal Server Error", null, null, null);
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(ex));

        assertThrows(RetryableException.class, () -> client.deliver(dto));
    }

    @Test
    void testDeliver_ConnectionRefused_ThrowsRetryable() {
        MessageDto dto = MessageDto.builder().id("1").payload("ok").timestamp(Instant.now()).build();
        WebClientRequestException ex = mock(WebClientRequestException.class);
        when(ex.getMessage()).thenReturn("Connection refused");
        when(responseSpec.toBodilessEntity()).thenReturn(Mono.error(ex));

        assertThrows(RetryableException.class, () -> client.deliver(dto));
    }
}
