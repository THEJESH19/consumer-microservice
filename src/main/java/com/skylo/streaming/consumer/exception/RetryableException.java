package com.skylo.streaming.consumer.exception;

/**
 * Custom exception representing a retryable failure (e.g., downstream service offline or HTTP 5xx).
 * Throwing this exception triggers partition pausing and fixed backoff retry loops.
 */
public class RetryableException extends RuntimeException {
    public RetryableException(String message) {
        super(message);
    }

    public RetryableException(String message, Throwable cause) {
        super(message, cause);
    }
}
