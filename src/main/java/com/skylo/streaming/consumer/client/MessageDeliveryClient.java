package com.skylo.streaming.consumer.client;

import com.skylo.streaming.consumer.dto.MessageDto;

/**
 * Interface defining the contract for message delivery downstream.
 */
public interface MessageDeliveryClient {

    /**
     * Delivers a message to the downstream target service.
     *
     * @param message the message payload to deliver
     */
    void deliver(MessageDto message);
}
