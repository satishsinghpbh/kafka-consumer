package com.github.satish.kafkaconsumer.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka Consumer Configuration
 * Configures the consumer factory, listener container factory, and error handling
 */
@Slf4j
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    @Value("${spring.kafka.consumer.max-poll-records:100}")
    private Integer maxPollRecords;

    @Value("${spring.kafka.consumer.max-poll-interval-ms:300000}")
    private Integer maxPollIntervalMs;

    @Value("${spring.kafka.listener.concurrency:3}")
    private Integer concurrency;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * Create Kafka consumer factory with error handling deserializer
     *
     * @return ConsumerFactory configured for JSON deserialization with error handling
     */
    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> props = new HashMap<>();

        // Broker connection settings
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getConsumer().getGroupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaProperties.getConsumer().getClientId());

        // Deserialization settings
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class.getName());

        // JSON deserialization properties
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Object.class.getName());
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.github.satish.kafkaconsumer.model");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);

        // Offset management
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
                kafkaProperties.getConsumer().getAutoOffsetReset().toString().toLowerCase());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual acknowledgment

        // Performance tuning
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, maxPollIntervalMs);

        // Network & reliability
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60000);

        // Partition assignment strategy for zero-downtime deployments
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

        // Static group membership to prevent rebalances on pod restarts
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, 
                System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME") : "payment-service-consumer-1");

        // Isolation level for exactly-once semantics (when producer is transactional)
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        log.info("Kafka consumer factory configured with properties: bootstrap-servers={}, group-id={}, " +
                "max-poll-records={}, concurrency={}",
                kafkaProperties.getBootstrapServers(),
                kafkaProperties.getConsumer().getGroupId(),
                maxPollRecords,
                concurrency);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    /**
     * Create Kafka listener container factory with error handling
     *
     * @return ConcurrentKafkaListenerContainerFactory configured with DefaultErrorHandler
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory);

        // Concurrency - number of threads for parallel message processing
        factory.setConcurrency(concurrency);

        // Manual acknowledgment mode - commit offset only after successful processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Poll timeout - how long container blocks on poll() waiting for records
        factory.getContainerProperties().setPollTimeout(3000);

        // Enable observation/tracing for Micrometer
        factory.getContainerProperties().setObservationEnabled(true);

        // Error handling with exponential backoff and DLT
        factory.setCommonErrorHandler(kafkaErrorHandler());

        log.info("Kafka listener container factory configured: concurrency={}, ack-mode=MANUAL_IMMEDIATE",
                concurrency);

        return factory;
    }

    /**
     * Create Kafka error handler with exponential backoff and dead letter publishing
     *
     * @return DefaultErrorHandler configured with retries and DLT
     */
    @Bean
    public org.springframework.kafka.listener.DefaultErrorHandler kafkaErrorHandler() {
        // Exponential backoff: 1s -> 2s -> 4s
        org.springframework.kafka.listener.ExponentialBackOff backOff =
                new org.springframework.kafka.listener.ExponentialBackOff(1000, 2.0);
        backOff.setMaxInterval(4000);

        org.springframework.kafka.listener.DefaultErrorHandler errorHandler =
                new org.springframework.kafka.listener.DefaultErrorHandler(
                        new org.springframework.kafka.listener.DeadLetterPublishingRecoverer(
                                kafkaTemplate()
                        ),
                        backOff
                );

        // Log after 3 failed attempts
        errorHandler.setLogLevel(org.springframework.kafka.listener.LoggingLevel.ERROR);

        log.info("Kafka error handler configured with exponential backoff and DLT");

        return errorHandler;
    }

    /**
     * Create Kafka template for publishing dead letters
     *
     * @return KafkaTemplate for sending messages to DLT
     */
    @Bean
    public org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate() {
        return new org.springframework.kafka.core.KafkaTemplate<>(
                new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(producerConfigs())
        );
    }

    /**
     * Producer configuration for DLT
     *
     * @return Map of producer configurations
     */
    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaProperties.getBootstrapServers());
        props.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG, "all");
        props.put(org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG, 3);
        return props;
    }
}
