package com.github.satish.kafkaconsumer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Payment event domain model
 * Represents a payment transaction from Kafka topic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentEvent {

    @JsonProperty("payment_id")
    private String paymentId;

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("amount")
    private BigDecimal amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("status")
    private String status;

    @JsonProperty("description")
    private String description;

    @JsonProperty("created_at")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime createdAt;

    @JsonProperty("updated_at")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime updatedAt;

    @JsonProperty("payment_method")
    private String paymentMethod;

    @JsonProperty("transaction_reference")
    private String transactionReference;
}
