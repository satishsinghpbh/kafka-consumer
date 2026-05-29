package com.github.satish.kafkaconsumer.service;

import com.github.satish.kafkaconsumer.model.PaymentEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Payment processing service
 * Contains business logic for payment event handling
 */
@Slf4j
@Service
public class PaymentService {

    /**
     * Process payment event
     *
     * @param payment the payment event to process
     */
    public void processPayment(PaymentEvent payment) {
        log.info("Processing payment: paymentId={}, orderId={}, amount={}, currency={}, status={}",
                payment.getPaymentId(),
                payment.getOrderId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus());

        try {
            // Simulate payment processing
            validatePayment(payment);
            persistPayment(payment);
            notifyPaymentProcessed(payment);

            log.info("Payment processed successfully: paymentId={}", payment.getPaymentId());
        } catch (Exception e) {
            log.error("Error processing payment: paymentId={}, error={}",
                    payment.getPaymentId(), e.getMessage(), e);
            throw new PaymentProcessingException("Failed to process payment: " + payment.getPaymentId(), e);
        }
    }

    /**
     * Validate payment data
     *
     * @param payment the payment to validate
     */
    private void validatePayment(PaymentEvent payment) {
        if (payment.getAmount().compareTo(java.math.BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Invalid payment amount: " + payment.getAmount());
        }
        if (payment.getPaymentId() == null || payment.getPaymentId().isBlank()) {
            throw new IllegalArgumentException("Payment ID cannot be null or empty");
        }
        log.debug("Payment validation passed: paymentId={}", payment.getPaymentId());
    }

    /**
     * Persist payment to database (placeholder)
     *
     * @param payment the payment to persist
     */
    private void persistPayment(PaymentEvent payment) {
        // TODO: Implement database persistence
        log.debug("Payment persisted: paymentId={}", payment.getPaymentId());
    }

    /**
     * Notify payment processed (placeholder)
     *
     * @param payment the processed payment
     */
    private void notifyPaymentProcessed(PaymentEvent payment) {
        // TODO: Implement notification logic (email, webhook, etc.)
        log.debug("Payment notification sent: paymentId={}", payment.getPaymentId());
    }

    /**
     * Custom exception for payment processing errors
     */
    public static class PaymentProcessingException extends RuntimeException {
        public PaymentProcessingException(String message, Throwable cause) {
            super(message, cause);
        }

        public PaymentProcessingException(String message) {
            super(message);
        }
    }
}
