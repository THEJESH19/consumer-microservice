package com.skylo.streaming.consumer;

import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaZKBroker;

/**
 * Utility to run an in-memory Kafka broker on port 9092.
 * Used for running and dry-testing the pipeline locally when Docker/Kafka is not installed on the host.
 */
public class EmbeddedKafkaServer {
    public static void main(String[] args) {
        System.out.println("==================================================");
        System.out.println("Starting Embedded Kafka Broker on port 9092...");
        System.out.println("==================================================");
        
        try {
            // Instantiate an embedded ZK-backed Kafka broker with 1 node, exposing the 'pipeline-messages' topic on port 9092
            EmbeddedKafkaZKBroker broker = new EmbeddedKafkaZKBroker(1, true, "pipeline-messages")
                    .kafkaPorts(9092);
            
            // Set properties and start
            broker.afterPropertiesSet();
            
            System.out.println("==================================================");
            System.out.println("Embedded Kafka Broker started successfully on port 9092!");
            System.out.println("Bootstrap servers: localhost:9092");
            System.out.println("Keep this window open to keep Kafka running.");
            System.out.println("==================================================");
            
            // Keep the main thread alive
            synchronized (EmbeddedKafkaServer.class) {
                EmbeddedKafkaServer.class.wait();
            }
        } catch (Exception e) {
            System.err.println("Failed to start Embedded Kafka: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
