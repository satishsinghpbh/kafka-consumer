package com.github.satish.kafkaconsumer.listener;

import com.github.satish.kafkaconsumer.model.OrderEvent;
import com.github.satish.kafkaconsumer.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka listener for order events
 * Consumes messages from order-events topic with manual acknowledgment
 */
@Slf4j
@Component
public class OrderEventListener {

    private final OrderService orderService;

    public OrderEventListener(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Listen to order events from Kafka topic
     * Uses MANUAL_IMMEDIATE acknowledgment mode for at-least-once delivery guarantee
     *
     * @param order the order event payload
     * @param acknowledgment manual acknowledgment object
     * @param partition the partition from which message was consumed
     * @param offset the offset of the message
     */
    @KafkaListener(
            topics = "order-events",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenOrderEvents(
            @Payload OrderEvent order,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        try {
            log.info("Received order event: orderId={}, customerId={}, partition={}, offset={}, timestamp={}",
                    order.getOrderId(), order.getCustomerId(), partition, offset, System.currentTimeMillis());

            // Process the order event
            orderService.processOrder(order);

            // Manual acknowledgment - commit offset only after successful processing
            acknowledgment.acknowledge();
            log.debug("Order event acknowledged: orderId={}, offset={}", order.getOrderId(), offset);

        } catch (Exception e) {
            log.error("Error processing order event: orderId={}, partition={}, offset={}, error={}",
                    order.getOrderId(), partition, offset, e.getMessage(), e);
            // Do NOT acknowledge - message will be retried by DefaultErrorHandler
            throw e;
        }
    }

    /**
     * Listen to dead letter topic for failed order events
     * These are messages that failed processing after all retries
     *
     * @param order the failed order event
     * @param exception the exception that caused the failure
     * @param partition the partition from which message was consumed
     * @param offset the offset of the message
     */
    @KafkaListener(
            topics = "order-events.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void listenOrderEventsDLT(
            @Payload OrderEvent order,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exception,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.error("Order event moved to DLT: orderId={}, partition={}, offset={}, exception={}",
                order.getOrderId(), partition, offset, exception);

        // TODO: Implement DLT handling logic
        // - Store failed order for manual review
        // - Alert operations team
        // - Create incident ticket
    }
}
