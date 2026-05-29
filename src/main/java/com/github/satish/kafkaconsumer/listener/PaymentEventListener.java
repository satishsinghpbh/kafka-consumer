package com.github.satish.kafkaconsumer.listener;

import com.github.satish.kafkaconsumer.model.PaymentEvent;
import com.github.satish.kafkaconsumer.service.PaymentService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Kafka listener for payment events
 * Consumes messages from payment-events topic with manual acknowledgment
 */
@Slf4j
@Component
public class PaymentEventListener {

    private final PaymentService paymentService;

    public PaymentEventListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    /**
     * Listen to payment events from Kafka topic
     * Uses MANUAL_IMMEDIATE acknowledgment mode for at-least-once delivery guarantee
     *
     * @param payment the payment event payload
     * @param acknowledgment manual acknowledgment object
     * @param partition the partition from which message was consumed
     * @param offset the offset of the message
     */
    @KafkaListener(
            topics = "payment-events",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void listenPaymentEvents(
            @Payload PaymentEvent payment,
            Acknowledgment acknowledgment,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        try {
            log.info("Received payment event: paymentId={}, partition={}, offset={}, timestamp={}",
                    payment.getPaymentId(), partition, offset, System.currentTimeMillis());

            // Process the payment event
            paymentService.processPayment(payment);

            // Manual acknowledgment - commit offset only after successful processing
            acknowledgment.acknowledge();
            log.debug("Payment event acknowledged: paymentId={}, offset={}", payment.getPaymentId(), offset);

        } catch (Exception e) {
            log.error("Error processing payment event: paymentId={}, partition={}, offset={}, error={}",
                    payment.getPaymentId(), partition, offset, e.getMessage(), e);
            // Do NOT acknowledge - message will be retried by DefaultErrorHandler
            throw e;
        }
    }

    /**
     * Listen to dead letter topic for failed payment events
     * These are messages that failed processing after all retries
     *
     * @param payment the failed payment event
     * @param exception the exception that caused the failure
     * @param partition the partition from which message was consumed
     * @param offset the offset of the message
     */
    @KafkaListener(
            topics = "payment-events.DLT",
            groupId = "${spring.kafka.consumer.group-id}-dlt"
    )
    public void listenPaymentEventsDLT(
            @Payload PaymentEvent payment,
            @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exception,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        log.error("Payment event moved to DLT: paymentId={}, partition={}, offset={}, exception={}",
                payment.getPaymentId(), partition, offset, exception);

        // TODO: Implement DLT handling logic
        // - Store failed payment for manual review
        // - Alert operations team
        // - Create incident ticket
    }
}
