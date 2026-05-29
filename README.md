# =============================================================================
# Spring Boot Kafka Consumer — Production application.properties Reference
# Spring Boot 3.x / Spring Kafka 3.x / Apache Kafka 3.x
#
# Every property includes:
#   Default : Spring Boot / Kafka client out-of-the-box default
#   Prod    : recommended production value + reason
#
# Sections:
#   1.  Broker Connection
#   2.  Security (SSL / SASL)
#   3.  Consumer — Identity
#   4.  Consumer — Offset Management
#   5.  Consumer — Serialization / Deserialization
#   6.  Consumer — Fetch & Performance
#   7.  Consumer — Network & Reliability
#   8.  Consumer — Partition Assignment Strategy
#   9.  Consumer — Isolation Level (Exactly-Once)
#  10.  Listener Container
#  11.  Observability (Actuator / Metrics / Tracing)
#  12.  Schema Registry (Avro / Protobuf — if used)
#  13.  Local Development Overrides
#  14.  Critical Rules Summary
# =============================================================================


# =============================================================================
# 1. BROKER CONNECTION
# =============================================================================

# Default : localhost:9092
# Prod    : always list 3+ brokers for high availability.
#           A single broker = single point of failure (SPOF).
#           Use DNS names, not IPs — IPs change; DNS absorbs broker restarts.
spring.kafka.bootstrap-servers=broker1.internal:9092,broker2.internal:9092,broker3.internal:9092


# =============================================================================
# 2. SECURITY (SSL / SASL)
# =============================================================================

# Default : PLAINTEXT (no encryption, no authentication)
# Prod    : SASL_SSL — encrypts in-transit AND authenticates the client.
#           Options: PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL
#           NEVER use PLAINTEXT or SASL_PLAINTEXT in production.
spring.kafka.security.protocol=SASL_SSL

# Default : (none)
# Prod    : PLAIN is widely supported.
#           Prefer SCRAM-SHA-512 for stronger, credential-rotation-friendly auth.
#           For mTLS-only auth, omit sasl.* and configure ssl.keystore.* instead.
spring.kafka.properties.sasl.mechanism=SCRAM-SHA-512

# Default : (none)
# Prod    : NEVER hardcode credentials here.
#           Inject via environment variables or a secrets manager (Vault, AWS Secrets Manager).
spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_USERNAME}" password="${KAFKA_PASSWORD}";

# Default : https
# Prod    : https — verifies broker certificate hostname matches CN/SAN.
#           NEVER set to "" (empty string) — disables hostname verification → MITM risk.
spring.kafka.properties.ssl.endpoint.identification.algorithm=https

# Default : (none)
# Prod    : trust store validates the broker's certificate chain.
#           Use JKS or PKCS12; store password in vault, never in source code.
spring.kafka.properties.ssl.truststore.type=JKS
spring.kafka.properties.ssl.truststore.location=/etc/kafka/certs/kafka.truststore.jks
spring.kafka.properties.ssl.truststore.password=${KAFKA_TRUSTSTORE_PASSWORD}

# Default : (none)
# Prod    : key store is required ONLY for mTLS (mutual TLS) — when the broker
#           requires client certificates in addition to SASL.
#           Omit these three lines if you are using SASL alone.
spring.kafka.properties.ssl.keystore.type=JKS
spring.kafka.properties.ssl.keystore.location=/etc/kafka/certs/kafka.keystore.jks
spring.kafka.properties.ssl.keystore.password=${KAFKA_KEYSTORE_PASSWORD}
spring.kafka.properties.ssl.key.password=${KAFKA_KEY_PASSWORD}

# Default : TLSv1.2,TLSv1.3
# Prod    : TLSv1.2,TLSv1.3 — explicitly disable TLS 1.0 and 1.1.
#           Some Kafka distributions still have older defaults.
spring.kafka.properties.ssl.enabled.protocols=TLSv1.2,TLSv1.3


# =============================================================================
# 3. CONSUMER — IDENTITY
# =============================================================================

# Default : (none) — required; app will fail at startup without it
# Prod    : unique, descriptive name per service.
#           All instances of the SAME service share one group-id (that is correct).
#           NEVER share group-id across DIFFERENT services — they would steal
#           each other's partition assignments silently.
spring.kafka.consumer.group-id=payment-service-consumer-group

# Default : auto-generated (e.g. "consumer-1")
# Prod    : set explicitly — makes broker logs, consumer group describes,
#           and Micrometer metrics human-readable.
#           In Kubernetes: append pod name via env var for per-instance tracing.
#           e.g. payment-service-consumer-${HOSTNAME}
spring.kafka.consumer.client-id=payment-service-consumer


# =============================================================================
# 4. CONSUMER — OFFSET MANAGEMENT
# =============================================================================

# Default : latest  (skips all messages produced before this consumer group existed)
# Prod    : earliest — guarantees no message is missed on first deploy or after
#           a consumer group offset is lost/reset.
#           Use 'latest' ONLY when you explicitly and intentionally accept data loss
#           for messages produced while the consumer was down.
#           NEVER use 'none' unless you fully own OffsetOutOfRangeException handling.
spring.kafka.consumer.auto-offset-reset=earliest

# Default : true — offsets committed automatically every 5 s in the background
# Prod    : false — mandatory when using Spring Kafka AckMode.
#           auto-commit=true with AckMode causes:
#             • data loss   : offset committed before your processing completes
#             • duplicates  : offset lost in-flight on crash before commit window
#           These failures are SILENT — no exception is thrown.
spring.kafka.consumer.enable-auto-commit=false

# Default : 5000ms
# Prod    : N/A — only applies when enable-auto-commit=true.
#           Documented here for completeness; do not uncomment in production.
# spring.kafka.consumer.auto-commit-interval=1000ms

# Default : (not set — broker default is __consumer_offsets topic)
# Prod    : leave at default. Only override when connecting to a multi-tenant cluster
#           where your group uses a non-default offsets topic.
# spring.kafka.consumer.properties.offsets.topic.replication.factor=3


# =============================================================================
# 5. CONSUMER — SERIALIZATION / DESERIALIZATION
# =============================================================================

# Default : (none) — app will fail to start without both deserializers
# Prod    : match the producer's serializer type exactly.
spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer

# Default : (none)
# Prod    : JsonDeserializer for JSON. For Avro: io.confluent.kafka.serializers.KafkaAvroDeserializer
#           For plain String: StringDeserializer
spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer

# Default : (none) — deserialization throws IllegalArgumentException without this
# Prod    : list exact packages your DTOs live in.
#           NEVER use '*' — it allows any class to be instantiated during deserialization
#           which is a remote code execution risk.
spring.kafka.consumer.properties.spring.json.trusted.packages=com.mycompany.myapp.model,com.mycompany.myapp.dto

# Default : false
# Prod    : true — honour the __TypeId__ header the producer embeds.
#           Without this, JsonDeserializer always deserializes into one fixed target type.
#           Required for polymorphic event types on the same topic.
spring.kafka.consumer.properties.spring.json.use.type.headers=true

# Default : (not set)
# Prod    : set the fallback target type when the producer does NOT send a __TypeId__ header.
#           Guards against producers that were not built with Spring Kafka.
spring.kafka.consumer.properties.spring.json.value.default.type=com.mycompany.myapp.dto.PaymentEvent

# Default : (not set)
# Prod    : map inbound __TypeId__ values to local classes when producer and consumer
#           live in different packages or repos.
#           Format: fullyQualifiedProducerClass:fullyQualifiedConsumerClass,...
# spring.kafka.consumer.properties.spring.json.type.mapping=com.producer.Event:com.mycompany.myapp.dto.PaymentEvent

# DESERIALIZATION ERROR HANDLING:
# Default : DeserializationException is thrown → error handler retries forever
# Prod    : configure an ErrorHandlingDeserializer as a wrapper so that
#           deserialization failures are sent to the DLT instead of parking the partition.
#           Do this in Java config (see KafkaConsumerConfig.java).
#           Set via factory bean — not via properties.


# =============================================================================
# 6. CONSUMER — FETCH & PERFORMANCE
# =============================================================================

# Default : 500
# Prod    : tune based on message weight and per-record processing time.
#           Lightweight processing (DB writes, cache updates)  → 100–500
#           Heavy processing (external HTTP, complex transforms) → 10–100
#           High-throughput streaming / analytics pipelines    → 500–2000
#
#   GOLDEN RULE: max-poll-records × avg-processing-time-ms < max.poll.interval.ms
#   Breach this → broker declares consumer dead → rebalance storm that never recovers.
spring.kafka.consumer.max-poll-records=100

# Default : 1 byte
# Prod    : 1024 bytes (1 KB) — broker waits until at least this much data is
#           available before responding. Reduces fetch round-trips and broker CPU.
#           Increase to 65536 (64 KB) for high-throughput topics with small messages.
spring.kafka.consumer.fetch-min-size=1024

# Default : 500ms
# Prod    : 500ms — max time broker will wait before responding even when
#           fetch-min-size is not met. Controls the latency ceiling.
#           Increase (e.g. 1000ms) only for batch analytics that accept higher latency.
spring.kafka.consumer.fetch-max-wait=500ms

# Default : 52428800 bytes (50 MB)
# Prod    : 52428800 — max total data returned across all partitions in one fetch.
#           Reduce only if consumer JVM heap is constrained.
spring.kafka.consumer.properties.fetch.max.bytes=52428800

# Default : 1048576 bytes (1 MB)
# Prod    : 1048576 — max data per partition per fetch.
#           Must be >= broker's max.message.bytes setting.
#           Increase only when individual messages exceed 1 MB.
spring.kafka.consumer.properties.max.partition.fetch.bytes=1048576


# =============================================================================
# 7. CONSUMER — NETWORK & RELIABILITY
# =============================================================================

# Default : 300000ms (5 minutes)
# Prod    : 300000ms — max time between poll() calls before broker considers
#           the consumer dead and triggers a group rebalance.
#           Increase ONLY when batch processing genuinely exceeds 5 min.
#           Obey: max-poll-records × avg-process-time-ms < this value at all times.
spring.kafka.consumer.properties.max.poll.interval.ms=300000

# Default : 45000ms
# Prod    : 30000ms — broker waits this long without a heartbeat before evicting
#           the consumer from the group. Must be within broker bounds:
#           [group.min.session.timeout.ms (6 s), group.max.session.timeout.ms (30 min)].
#           Lower = faster failure detection; too low = false evictions on long GC pauses.
spring.kafka.consumer.properties.session.timeout.ms=30000

# Default : 3000ms
# Prod    : 10000ms — heartbeat send interval.
#           Rule: heartbeat.interval.ms must be < session.timeout.ms / 3.
#           10000 / 30000 = 1/3 — satisfies the rule exactly.
spring.kafka.consumer.properties.heartbeat.interval.ms=10000

# Default : 30000ms
# Prod    : 30000ms — how long the client waits for any broker response.
#           Increase only for cross-region or high-latency network paths.
spring.kafka.consumer.properties.request.timeout.ms=30000

# Default : 60000ms (1 minute)
# Prod    : 60000ms — broker has this long to complete metadata, offset commit,
#           and other admin requests before the client times out.
spring.kafka.consumer.properties.default.api.timeout.ms=60000

# Default : 50ms
# Prod    : 100ms — initial back-off before reconnecting to a failed broker.
#           Grows exponentially up to reconnect.backoff.max.ms.
spring.kafka.consumer.properties.reconnect.backoff.ms=100

# Default : 1000ms
# Prod    : 10000ms — cap on reconnect back-off. Prevents reconnect storms
#           when many consumers restart simultaneously after a broker outage.
spring.kafka.consumer.properties.reconnect.backoff.max.ms=10000

# Default : 2147483647 (unlimited)
# Prod    : 3 — retry transient network-level send errors before failing.
#           Note: these are low-level client retries, separate from Spring Kafka
#           listener-level retries (configured in DefaultErrorHandler).
spring.kafka.consumer.properties.retries=3

# Default : 100ms
# Prod    : 1000ms — wait between low-level client retries.
spring.kafka.consumer.properties.retry.backoff.ms=1000

# Default : 540000ms (9 minutes)
# Prod    : 30000ms — caps the total retry duration for a single request.
#           Prevents one stuck request from blocking the consumer indefinitely.
spring.kafka.consumer.properties.delivery.timeout.ms=30000

# Default : 300000ms
# Prod    : 300000ms — max time the consumer waits for a response to
#           a metadata refresh request. Usually does not need tuning.
spring.kafka.consumer.properties.metadata.max.age.ms=300000

# Default : 9 (max connections per broker)
# Prod    : 9 — rarely needs changing. Increase only for very high-throughput
#           consumers to saturate multi-partition brokers.
# spring.kafka.consumer.properties.connections.max.idle.ms=540000

# Default : (socket default)
# Prod    : 32768 bytes (32 KB) — receive buffer.
#           Set to -1 to use the OS default, which is usually appropriate.
spring.kafka.consumer.properties.receive.buffer.bytes=65536


# =============================================================================
# 8. CONSUMER — PARTITION ASSIGNMENT STRATEGY
# =============================================================================

# Default : RangeAssignor
# Prod    : CooperativeStickyAssignor — enables incremental cooperative rebalancing.
#           Unlike the default EagerAssignor strategies (Range, RoundRobin):
#             • Only partitions that need to move are revoked during a rebalance.
#             • Consumers keep their assigned partitions while rebalancing.
#             • Zero-downtime rolling restarts and deployments.
#           WARNING: all consumers in the group must use the same assignor.
#           Migrate from eager → cooperative in two rolling-restart phases
#           (add CooperativeStickyAssignor alongside old assignor, then remove old).
spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# Default : (not set)
# Prod    : set group.instance.id to enable Static Group Membership.
#           Prevents unnecessary rebalances when consumers restart within
#           session.timeout.ms (e.g. rolling pod restart in Kubernetes).
#           Each instance must have a UNIQUE, STABLE identifier (e.g. pod name).
#           Inject via environment variable in your Kubernetes Deployment spec.
spring.kafka.consumer.properties.group.instance.id=${HOSTNAME:payment-service-consumer-static}


# =============================================================================
# 9. CONSUMER — ISOLATION LEVEL (Exactly-Once / Transactional Producers)
# =============================================================================

# Default : read_uncommitted
# Prod    : read_committed — ONLY when your producer uses Kafka transactions
#           (producer.transaction-id-prefix is set on the producer side).
#           read_committed makes the consumer skip transactional messages that
#           have not yet been committed by the producer, preventing dirty reads.
#           Do NOT set this if your producer is non-transactional —
#           it will cause the consumer to lag behind indefinitely waiting for
#           commit markers that never arrive.
spring.kafka.consumer.properties.isolation.level=read_committed


# =============================================================================
# 10. LISTENER CONTAINER
# =============================================================================

# Default : BATCH
# Prod    : MANUAL_IMMEDIATE — offset committed only after your @KafkaListener
#           method explicitly calls Acknowledgment.acknowledge().
#           Strongest at-least-once delivery guarantee in Spring Kafka.
#
#   Full option list:
#     RECORD           — auto-commit after each record returns from listener
#     BATCH            — auto-commit after the full poll batch returns
#     TIME             — commit on a fixed time schedule
#     COUNT            — commit after N records
#     COUNT_TIME       — commit on whichever of COUNT or TIME fires first
#     MANUAL           — queues acks; flushes at end of batch
#     MANUAL_IMMEDIATE — commits immediately when acknowledge() is called  ← use this
spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE

# Default : 1
# Prod    : set equal to the number of topic partitions.
#           concurrency > partitions → excess threads are permanently idle (wasted).
#           concurrency < partitions → some partitions share threads (lower throughput).
#           For multiple topics with different partition counts, set the highest count
#           and let idle threads park naturally on smaller topics.
spring.kafka.listener.concurrency=3

# Default : 3000ms
# Prod    : 3000ms — how long the container blocks on poll() waiting for records.
#           Increase to 5000ms for very low-traffic topics to reduce CPU spinning.
#           Decrease for extremely latency-sensitive consumers.
spring.kafka.listener.poll-timeout=3000ms

# Default : SINGLE (one ConsumerRecord per listener invocation)
# Prod    : SINGLE for most use cases — simplest error handling and acknowledgment.
#           Use BATCH only for bulk-write pipelines where you manage the full
#           List<ConsumerRecord<?,?>> yourself and handle partial-batch failures.
spring.kafka.listener.type=SINGLE

# Default : true
# Prod    : true — application refuses to start if subscribed topics do not exist.
#           Catches misconfigured topic names at deploy time, not silently at runtime.
#           Set false ONLY in local dev where auto-topic-creation is enabled.
spring.kafka.listener.missing-topics-fatal=true

# Default : (not set)
# Prod    : 60000ms — fires an IdleListenerContainerEvent when no records arrive
#           within this window. Listen to this event to alert on stuck consumers,
#           lag spikes, or unexpectedly empty topics in production.
spring.kafka.listener.idle-event-interval=60000ms

# Default : (not set)
# Prod    : 30s — how often the listener container samples consumer lag.
#           Required to expose kafka.consumer.lag Micrometer metric for dashboards.
spring.kafka.listener.monitor-interval=30s

# Default : 30s
# Prod    : 30s — max time the listener container waits for the underlying
#           consumer thread to start before throwing an error. Increase only
#           if broker authentication (SASL) causes slow initial handshakes.
spring.kafka.listener.consumer-start-timeout=30s

# Default : (not set — no sub-batch filtering)
# Prod    : configure a RecordFilterStrategy @Bean in Java to silently skip
#           messages that don't match a predicate (e.g. wrong event type, old schema version).
#           This is a Java-only config — not configurable via properties.
#           See KafkaConsumerConfig.java → factory.setRecordFilterStrategy(...)

# Default : (not set — no interceptors)
# Prod    : wire a ConsumerInterceptor @Bean for cross-cutting concerns:
#           distributed tracing (Micrometer Observation / Brave), audit logging.
#           Spring Boot auto-configures Micrometer tracing interceptor when
#           spring-boot-starter-actuator + micrometer-tracing-bridge-* are on classpath.

# Default : (not set)
# Prod    : true — enables Micrometer Observation instrumentation on the listener container.
#           Automatically creates kafka.consumer.* metrics and distributed trace spans.
#           Requires micrometer-tracing on the classpath.
spring.kafka.listener.observation-enabled=true


# =============================================================================
# 11. OBSERVABILITY (Actuator / Metrics / Tracing)
# =============================================================================

# Default : health, info only
# Prod    : expose prometheus for Prometheus scraping; env for config audit;
#           loggers for runtime log-level changes without restart.
management.endpoints.web.exposure.include=health,info,metrics,prometheus,loggers,env

# Default : false
# Prod    : true — Kafka broker reachability appears in /actuator/health.
#           Kubernetes readiness probe should hit /actuator/health so the pod
#           is removed from service if brokers are unreachable.
management.health.kafka.enabled=true

# Default : 10s
# Prod    : 5s — Kafka health check probe timeout.
#           Must be shorter than your readiness probe failureThreshold × periodSeconds.
#           A hung broker should not stall the health endpoint indefinitely.
management.health.kafka.response-timeout=5s

# Default : (not set)
# Prod    : tag every Micrometer metric with the application name.
#           Critical for multi-service Prometheus deployments and Grafana filtering.
management.metrics.tags.application=${spring.application.name}

# Default : (not set)
# Prod    : enable Micrometer tracing for distributed trace propagation.
#           Requires micrometer-tracing-bridge-brave (Zipkin) or
#           micrometer-tracing-bridge-otel (OpenTelemetry) on classpath.
management.tracing.enabled=true

# Default : 0.1 (10% sampling)
# Prod    : 0.1 for high-volume services; 1.0 only in dev or low-traffic services.
#           At 1.0 on a 10k msg/s consumer you generate 10k spans/s — plan accordingly.
management.tracing.sampling.probability=0.1

# Default : (not set)
# Prod    : set application name for correct service attribution in Jaeger / Zipkin.
spring.application.name=payment-service


# =============================================================================
# 12. SCHEMA REGISTRY (Avro / Protobuf — include ONLY if you use Confluent SR)
# =============================================================================

# Uncomment this entire section if your value-deserializer is KafkaAvroDeserializer.
# Remove spring.json.* properties above when using Avro/Protobuf.

# spring.kafka.consumer.value-deserializer=io.confluent.kafka.serializers.KafkaAvroDeserializer
# spring.kafka.consumer.properties.schema.registry.url=https://schema-registry.internal:8081
# spring.kafka.consumer.properties.schema.registry.ssl.truststore.location=/etc/kafka/certs/sr.truststore.jks
# spring.kafka.consumer.properties.schema.registry.ssl.truststore.password=${SR_TRUSTSTORE_PASSWORD}
# spring.kafka.consumer.properties.basic.auth.credentials.source=USER_INFO
# spring.kafka.consumer.properties.basic.auth.user.info=${SR_USERNAME}:${SR_PASSWORD}

# Default : false
# Prod    : true — validate that consumed messages conform to the registered schema.
#           Prevents schema drift between producers and consumers from causing silent corruption.
# spring.kafka.consumer.properties.specific.avro.reader=true


# =============================================================================
# 13. LOCAL DEVELOPMENT OVERRIDES
# Save these in: src/main/resources/application-local.properties
# Activate with: -Dspring.profiles.active=local  OR  SPRING_PROFILES_ACTIVE=local
# =============================================================================

# spring.kafka.bootstrap-servers=localhost:9092
# spring.kafka.security.protocol=PLAINTEXT
# spring.kafka.consumer.group-id=my-app-dev-group
# spring.kafka.consumer.auto-offset-reset=earliest
# spring.kafka.consumer.enable-auto-commit=false
# spring.kafka.consumer.key-deserializer=org.apache.kafka.common.serialization.StringDeserializer
# spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer
# spring.kafka.consumer.properties.spring.json.trusted.packages=com.mycompany.myapp.model
# spring.kafka.consumer.properties.max.poll.interval.ms=300000
# spring.kafka.consumer.properties.isolation.level=read_uncommitted
# spring.kafka.consumer.properties.partition.assignment.strategy=org.apache.kafka.clients.consumer.RangeAssignor
# spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE
# spring.kafka.listener.concurrency=1
# spring.kafka.listener.missing-topics-fatal=false
# spring.kafka.listener.observation-enabled=false
# management.tracing.enabled=false


# =============================================================================
# 14. CRITICAL RULES SUMMARY — violating these causes silent production failures
# =============================================================================
#
#  FORMULA 1 — Poll interval (most commonly violated):
#    max-poll-records × avg-processing-time-ms < max.poll.interval.ms
#    Breach → broker evicts the consumer → rebalance loop → zero throughput
#    Fix   → reduce max-poll-records BEFORE increasing max.poll.interval.ms
#
#  FORMULA 2 — Heartbeat ratio:
#    heartbeat.interval.ms < session.timeout.ms / 3
#    Breach → false evictions under GC pressure, or slow failure detection
#
#  FORMULA 3 — Concurrency ceiling:
#    listener.concurrency <= number of topic partitions
#    Breach → excess threads permanently idle, wasting memory and scheduling
#
#  RULE 4 — Always wire a DefaultErrorHandler @Bean with:
#    • ExponentialBackOff (3 retries: 1s → 2s → 4s)
#    • DeadLetterPublishingRecoverer → sends poison pills to <topic>.DLT
#    Without DLT → one bad message stalls the partition forever
#
#  RULE 5 — Wrap deserializers in ErrorHandlingDeserializer:
#    factory.setValueDeserializer(new ErrorHandlingDeserializer<>(new JsonDeserializer<>(...)))
#    Without this → deserialization failure bypasses DefaultErrorHandler
#    and retries the bad bytes infinitely
#
#  RULE 6 — Use CooperativeStickyAssignor for zero-downtime deployments:
#    Default RangeAssignor stops ALL consumers briefly on every rebalance
#    CooperativeStickyAssignor only moves partitions that actually need to move
#
#  RULE 7 — Set group.instance.id for Static Group Membership in Kubernetes:
#    Prevents rebalance on every rolling pod restart within session.timeout.ms
#
#  RULE 8 — isolation.level=read_committed only when producer is transactional:
#    Setting this without transactional producers causes the consumer to stall
#    waiting for commit markers that never come
#
#  RULE 9 — NEVER:
#    • enable-auto-commit=true with Spring Kafka AckMode
#    • spring.json.trusted.packages=*  (RCE risk)
#    • Hardcode credentials in this file (use ${ENV_VAR})
#    • Set ssl.endpoint.identification.algorithm= (empty) — disables hostname check
#    • Use PLAINTEXT or SASL_PLAINTEXT in production
#    • Share group-id across different applications
#    • Set concurrency higher than partition count
#
# =============================================================================
