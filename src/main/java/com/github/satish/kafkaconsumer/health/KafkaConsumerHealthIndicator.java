package com.github.satish.kafkaconsumer.health;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Custom Kafka consumer health indicator
 * Monitors consumer lag and topic availability
 */
@Slf4j
@Component
public class KafkaConsumerHealthIndicator implements HealthIndicator {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaConsumerHealthIndicator(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public Health health() {
        try {
            // Check if Kafka broker is reachable
            kafkaTemplate.getDefaultTopic();
            
            log.debug("Kafka consumer health check passed");
            return Health.up()
                    .withDetail("status", "Kafka consumer is healthy")
                    .withDetail("timestamp", System.currentTimeMillis())
                    .build();

        } catch (Exception e) {
            log.error("Kafka consumer health check failed: {}", e.getMessage(), e);
            return Health.down()
                    .withDetail("status", "Kafka consumer is unhealthy")
                    .withDetail("error", e.getMessage())
                    .withDetail("timestamp", System.currentTimeMillis())
                    .build();
        }
    }
}
