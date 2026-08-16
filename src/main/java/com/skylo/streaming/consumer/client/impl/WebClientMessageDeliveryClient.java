package com.skylo.streaming.consumer.client.impl;

import com.skylo.streaming.consumer.client.MessageDeliveryClient;
import com.skylo.streaming.consumer.dto.MessageDto;
import com.skylo.streaming.consumer.exception.NonRetryableException;
import com.skylo.streaming.consumer.exception.RetryableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;

@Service
@Slf4j
public class WebClientMessageDeliveryClient implements MessageDeliveryClient {

    private final WebClient webClient;

    public WebClientMessageDeliveryClient(
            WebClient.Builder webClientBuilder,
            @Value("${persister.url:http://localhost:8082}") String persisterUrl) {
        
        log.info("Initializing persistence client pointing to: {}", persisterUrl);
        this.webClient = webClientBuilder.baseUrl(persisterUrl).build();
    }

    @Override
    public void deliver(MessageDto message) {
        log.info("Forwarding message {} downstream...", message.getId());

        try {
            webClient.post()
                    .uri("/api/v1/persist-message")
                    .contentType(MediaType.APPLICATION_JSON)
                    .bodyValue(message)
                    .retrieve()
                    .toBodilessEntity()
                    .block(); // Block synchronously to maintain order during retry blocks

            log.info("Message {} successfully delivered to persister.", message.getId());

        } catch (WebClientResponseException e) {
            HttpStatus status = (HttpStatus) e.getStatusCode();
            log.warn("Persister returned status: {} for message {}", status, message.getId());

            // 208 represents duplicate check pass on the persister end, which is a success for the consumer
            if (status == HttpStatus.ALREADY_REPORTED) {
                log.info("Message {} was already processed. Continuing.", message.getId());
                return;
            }

            // Client errors (4xx) are non-retryable except timeouts or rate-limiting
            if (status.is4xxClientError() && status != HttpStatus.REQUEST_TIMEOUT && status != HttpStatus.TOO_MANY_REQUESTS) {
                throw new NonRetryableException("Downstream client error: " + status, e);
            }

            throw new RetryableException("Downstream server error: " + status, e);

        } catch (WebClientRequestException e) {
            log.warn("Network error connecting to persister for message {}: {}", message.getId(), e.getMessage());
            throw new RetryableException("Persistence endpoint unreachable: " + e.getMessage(), e);

        } catch (Exception e) {
            log.error("Unexpected error delivering message {}: {}", message.getId(), e.getMessage());
            throw new RetryableException("Unexpected delivery error: " + e.getMessage(), e);
        }
    }
}
