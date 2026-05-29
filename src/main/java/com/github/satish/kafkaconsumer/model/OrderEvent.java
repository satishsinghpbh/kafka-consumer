package com.github.satish.kafkaconsumer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Order event domain model
 * Represents an order event from Kafka topic
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {

    @JsonProperty("order_id")
    private String orderId;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("order_status")
    private String orderStatus;

    @JsonProperty("total_items")
    private Integer totalItems;

    @JsonProperty("order_date")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime orderDate;

    @JsonProperty("shipping_address")
    private String shippingAddress;

    @JsonProperty("tracking_number")
    private String trackingNumber;

    @JsonProperty("created_at")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime createdAt;
}
