package com.skylo.streaming.consumer.exception;

/**
 * Custom exception representing a non-retryable failure (e.g., malformed payload or HTTP 4xx).
 * Throwing this exception triggers immediate routing of the message to the Dead Letter Topic (DLT).
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }

    public NonRetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
