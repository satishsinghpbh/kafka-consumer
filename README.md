# =============================================================================
# Spring Boot Kafka Consumer — Production application.properties Reference
# Spring Boot 3.x / Spring Kafka 3.x / Apache Kafka 3.x
#
# Every property includes:
#   Default : Spring Boot out-of-the-box default
#   Prod    : recommended production value + reason
# =============================================================================


# =============================================================================
# BROKER CONNECTION
# =============================================================================

# Default : localhost:9092
# Prod    : always 3+ brokers for high availability; use DNS names, not IPs.
#           A single broker = single point of failure.
spring.kafka.bootstrap-servers=broker1.internal:9092,broker2.internal:9092,broker3.internal:9092


# =============================================================================
# SECURITY
# =============================================================================

# Default : PLAINTEXT (no encryption, no authentication)
# Prod    : SASL_SSL — encrypts traffic in-transit AND authenticates the client.
#           Never use PLAINTEXT in production.
spring.kafka.security.protocol=SASL_SSL

# Default : (none)
# Prod    : PLAIN is widely supported; use SCRAM-SHA-512 for stronger auth.
spring.kafka.properties.sasl.mechanism=PLAIN

# Default : (none)
# Prod    : inject credentials via environment variables — NEVER hardcode them.
#           Uses ${KAFKA_USERNAME} and ${KAFKA_PASSWORD} env vars.
spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";

# Default : (none)
# Prod    : path to the JKS trust store for SSL certificate validation.
#           Store the password in a secrets vault; inject via env var.
spring.kafka.properties.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
spring.kafka.properties.ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD}

# Default : https
# Prod    : https — validates that the broker hostname matches the certificate CN.
#           NEVER set this to empty string — disables hostname verification (MITM risk).
spring.kafka.properties.ssl.endpoint.identification.algorithm=https


# =============================================================================
# CONSUMER — IDENTITY
# =============================================================================

# Default : (none) — this is required; app will fail without it
# Prod    : unique, descriptive name per service.
#           All instances of the same service share one group-id.
#           NEVER share group-id across different applications.
spring.kafka.consumer.group-id=payment-service-consumer-group

# Default : auto-generated as "consumer-1", "consumer-2", etc.
# Prod    : set explicitly — makes broker logs and Kafka metrics readable.
#           Append instance ID in horizontally-scaled deployments.
spring.kafka.consumer.client-id=payment-service-consumer


# =============================================================================
# CONSUMER — OFFSET MANAGEMENT
# =============================================================================

# Default : latest  (skips all messages produced before the consumer started)
# Prod    : earliest — never miss messages on first deploy or after group offset loss.
#           Use 'latest' ONLY if you explicitly accept missing past messages.
#           NEVER use 'none' unless you handle OffsetOutOfRangeException yourself.
spring.kafka.consumer.auto-offset-reset=earliest

# Default : true  — offsets committed automatically every 5 s in background
# Prod    : false — Spring Kafka's AckMode manages commits precisely.
#           auto-commit=true causes:
#             - data loss  : offset committed before processing completes
#             - duplicates : offset lost in-flight on crash
spring.kafka.consumer.enable-auto-commit=false

# Default : 5000ms
# Prod    : 1000ms — only relevant if enable-auto-commit=true (avoid in prod).
#           Reduces duplicate-processing window if you must use auto-commit.
# spring.kafka.consumer.auto-commit-interval=1000ms


# =============================================================================
# CONSUMER — SERIALIZATION
# =============================================================================

# Default : (none) — must be set explicitly; app will fail to start without it
# Prod    : must match the producer's serializer exactly.
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Default : (none) — must be set explicitly
# Prod    : JsonDeserializer for JSON payloads.
#           For plain strings use StringDeserializer.
#           For Avro use KafkaAvroDeserializer (Confluent Schema Registry).
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# Default : (none) — deserialization will throw an exception without this
# Prod    : list your exact model packages.
#           NEVER use '*' in production — allows arbitrary class instantiation (security risk).
spring.kafka.consumer.properties.spring.json.trusted.packages=com.mycompany.myapp.model,com.mycompany.myapp.dto

# Default : false
# Prod    : true — use the type header embedded by the producer instead of a fixed target class.
#           Required when the producer and consumer share different type hierarchies.
spring.kafka.consumer.properties.spring.json.use.type.headers=true


# =============================================================================
# CONSUMER — FETCH PERFORMANCE
# =============================================================================

# Default : 500
# Prod    : tune based on message size and per-record processing time.
#           Lightweight processing (DB persist)       → 100–500
#           Heavy processing (external API calls)     → 10–50
#           High-throughput streaming pipelines       → 500–2000
#
#   CRITICAL FORMULA: max-poll-records × avg-processing-time < max.poll.interval.ms
#   Violating this causes the broker to declare the consumer dead → rebalance storm.
spring.kafka.consumer.max-poll-records=100

# Default : 1 byte
# Prod    : 1024 bytes (1 KB) — broker waits until at least this much data is ready
#           before responding. Reduces fetch round-trips and broker CPU load.
spring.kafka.consumer.fetch-min-size=1024

# Default : 500ms
# Prod    : 500ms — max time broker waits before responding even if fetch-min-size
#           is not met. Increase for better batching at the cost of latency.
spring.kafka.consumer.fetch-max-wait=500ms


# =============================================================================
# CONSUMER — RAW KAFKA CLIENT PROPERTIES
# (prefix: spring.kafka.consumer.properties.)
# =============================================================================

# Default : 300000ms (5 minutes)
# Prod    : 300000ms — max time between poll() calls before broker considers
#           the consumer dead and triggers a rebalance.
#           Increase ONLY if one batch genuinely takes more than 5 min to process.
#           Always satisfy: max-poll-records × avg-process-time-ms < this value.
spring.kafka.consumer.properties.max.poll.interval.ms=300000

# Default : 45000ms
# Prod    : 30000ms — time for the broker to detect a dead consumer via missed heartbeats.
#           Must be within broker's [group.min.session.timeout.ms=6000, group.max.session.timeout.ms=1800000].
#           Lower = faster failover after real failure.
#           Too low = false positives during long GC pauses.
spring.kafka.consumer.properties.session.timeout.ms=30000

# Default : 3000ms
# Prod    : 10000ms — how often the consumer sends heartbeats to the coordinator.
#           Rule: heartbeat.interval.ms ≈ session.timeout.ms / 3
#           Too low = wasted broker bandwidth.
#           Too high = risk of false session expiry.
spring.kafka.consumer.properties.heartbeat.interval.ms=10000

# Default : 30000ms
# Prod    : 30000ms — how long the client waits for a broker response before failing.
#           Increase only in high-latency environments (e.g. cross-region brokers).
spring.kafka.consumer.properties.request.timeout.ms=30000

# Default : 52428800 (50 MB)
# Prod    : 52428800 — max data returned per fetch across all partitions combined.
#           Reduce only if consumers face significant heap memory pressure.
spring.kafka.consumer.properties.fetch.max.bytes=52428800

# Default : 1048576 (1 MB)
# Prod    : 1048576 — max data fetched per partition per request.
#           Must be >= broker's max.message.bytes.
#           Increase only if you publish messages larger than 1 MB.
spring.kafka.consumer.properties.max.partition.fetch.bytes=1048576

# Default : 2147483647 (unlimited — effectively no retry)
# Prod    : 3 — retry transient network errors before failing.
#           Works with retry.backoff.ms below.
spring.kafka.consumer.properties.retries=3

# Default : 100ms
# Prod    : 1000ms — wait between retries.
#           Combined with retries=3: waits 1s, 2s, 4s before giving up.
spring.kafka.consumer.properties.retry.backoff.ms=1000

# Default : 540000ms (9 minutes)
# Prod    : 60000ms — max total retry time across all retry attempts.
#           Caps the retry loop so a hung network doesn't stall forever.
spring.kafka.consumer.properties.reconnect.backoff.max.ms=60000

# Default : 50ms
# Prod    : 100ms — initial wait before reconnecting to a failed broker.
#           Grows exponentially up to reconnect.backoff.max.ms.
spring.kafka.consumer.properties.reconnect.backoff.ms=100


# =============================================================================
# LISTENER CONTAINER
# =============================================================================

# Default : BATCH
# Prod    : MANUAL_IMMEDIATE — offset committed only after your listener calls
#           Acknowledgment.acknowledge() explicitly.
#           This is the strongest delivery guarantee available in Spring Kafka.
#
#   Options:
#     RECORD           — auto-commit after each record (no Acknowledgment call needed)
#     BATCH            — auto-commit after each entire poll batch
#     MANUAL           — queues commits; flushes at end of batch
#     MANUAL_IMMEDIATE — commits the moment acknowledge() is called  ← use this
spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE

# Default : 1
# Prod    : set equal to the number of topic partitions.
#           More threads than partitions = idle threads (wasted memory and scheduling).
#           Fewer threads than partitions = partitions share threads (reduced throughput).
#           Adjust per topic if you have multiple @KafkaListener topics.
spring.kafka.listener.concurrency=3

# Default : 3000ms
# Prod    : 3000ms — how long the container blocks waiting for records each poll cycle.
#           Increase to 5000ms for very low-traffic topics to reduce CPU spinning.
spring.kafka.listener.poll-timeout=3000ms

# Default : SINGLE
# Prod    : SINGLE — listener receives one ConsumerRecord at a time.
#           Use BATCH only for bulk-insert pipelines where you explicitly manage
#           a List<ConsumerRecord<?,?>> and handle partial-batch failures yourself.
spring.kafka.listener.type=SINGLE

# Default : true
# Prod    : true — application refuses to start if subscribed topics do not exist.
#           Catches misconfigured topic names at deploy time, not at runtime.
#           Set to false only in local dev where topics are auto-created lazily.
spring.kafka.listener.missing-topics-fatal=true

# Default : (not set)
# Prod    : 60000ms — fires an IdleListenerContainerEvent if no messages are
#           received within this window. Hook into this event to alert on
#           stuck consumers or unexpectedly empty topics.
spring.kafka.listener.idle-event-interval=60000ms

# Default : (not set)
# Prod    : 30s — how often the listener container checks consumer lag.
#           Exposes lag as a Micrometer metric for Prometheus / Grafana dashboards.
spring.kafka.listener.monitor-interval=30s

# Default : (not set)
# Prod    : 3 — number of times the container retries starting a failed consumer
#           thread before giving up and raising an error.
spring.kafka.listener.consumer-start-timeout=30s


# =============================================================================
# ACTUATOR — Health checks and metrics
# =============================================================================

# Expose health, metrics, and Prometheus scrape endpoint.
# Default : only 'health' and 'info' exposed
# Prod    : expose prometheus for scraping by Prometheus / Grafana stack.
management.endpoints.web.exposure.include=health,info,metrics,prometheus

# Default : false
# Prod    : true — includes Kafka broker reachability in /actuator/health.
#           Alerts your load balancer / orchestrator when brokers are unreachable.
management.health.kafka.enabled=true

# Default : 10s
# Prod    : 5s — timeout for the Kafka health check ping.
#           Keep short so a slow broker doesn't hang the health endpoint.
management.health.kafka.response-timeout=5s

# Default : (not set)
# Prod    : tag all Micrometer metrics with the application name.
#           Essential for filtering dashboards when multiple services share Prometheus.
management.metrics.tags.application=${spring.application.name}


# =============================================================================
# LOCAL DEVELOPMENT OVERRIDES
# (use in application-local.properties, NOT in production)
# =============================================================================

# Uncomment and place in src/main/resources/application-local.properties

# spring.kafka.bootstrap-servers=localhost:9092
# spring.kafka.security.protocol=PLAINTEXT
# spring.kafka.consumer.group-id=my-app-dev-group
# spring.kafka.consumer.auto-offset-reset=earliest
# spring.kafka.consumer.enable-auto-commit=false
# spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
# spring.kafka.consumer.properties.spring.json.trusted.packages=com.mycompany.myapp.model
# spring.kafka.consumer.properties.max.poll.interval.ms=300000
# spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
# spring.kafka.listener.concurrency=1
# spring.kafka.listener.missing-topics-fatal=false


# =============================================================================
# CRITICAL RULES — violating these causes silent failures
# =============================================================================
#
#  1. FORMULA — poll interval:
#       max-poll-records × avg-processing-time-ms < max.poll.interval.ms
#     Breach this → broker declares consumer dead → rebalance storm
#
#  2. FORMULA — heartbeat ratio:
#       heartbeat.interval.ms ≈ session.timeout.ms / 3
#     Breach this → false rebalances or slow dead-consumer detection
#
#  3. FORMULA — concurrency ceiling:
#       spring.kafka.listener.concurrency <= number of topic partitions
#     Exceeding this → extra threads are permanently idle
#
#  4. ALWAYS configure a DefaultErrorHandler @Bean with:
#       - ExponentialBackOff (retry 3 times)
#       - DeadLetterPublishingRecoverer (send to <topic>.DLT)
#     Without this → one bad message parks the partition forever
#
#  5. NEVER use enable-auto-commit=true with Spring Kafka's AckMode
#  6. NEVER use spring.json.trusted.packages=* in production
#  7. NEVER hardcode SASL credentials — use ${ENV_VAR} references
#  8. NEVER set ssl.endpoint.identification.algorithm= (empty) in production
#
# =============================================================================
