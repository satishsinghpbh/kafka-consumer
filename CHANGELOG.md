# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2026-05-29

### Added

#### Core Features
- Initial production-ready Kafka consumer implementation
- Support for multiple event types (PaymentEvent, OrderEvent)
- Manual acknowledgment mode (MANUAL_IMMEDIATE) for at-least-once delivery guarantee
- Configurable consumer with environment variable support
- Multi-profile support (dev, prod, test)

#### Error Handling & Resilience
- Exponential backoff retry strategy (1s → 2s → 4s)
- Dead Letter Topic (DLT) for failed messages
- DeadLetterPublishingRecoverer for poison pill handling
- ErrorHandlingDeserializer for safe message deserialization
- Comprehensive exception handling with custom exceptions

#### Performance & Scalability
- CooperativeStickyAssignor for zero-downtime deployments
- Static group membership to prevent unnecessary rebalances
- Configurable concurrency (default: 3 threads)
- Optimized batch sizes (max-poll-records: 100)
- G1GC garbage collector configuration

#### Observability & Monitoring
- Prometheus metrics endpoint
- Micrometer tracing integration (Brave/Zipkin ready)
- Custom health indicator for Kafka broker connectivity
- Actuator endpoints (health, metrics, prometheus, loggers, env)
- Structured logging with SLF4J

#### Infrastructure & Deployment
- Docker containerization with Alpine Linux
- Docker Compose for local development stack
- Kubernetes deployment manifests with best practices
- Pod Disruption Budget for high availability
- Security contexts and RBAC support
- Prometheus monitoring configuration

#### Documentation
- Comprehensive README with architecture guide
- BUILD.md with build and deployment instructions
- Inline code documentation with Javadoc
- Critical formulas for configuration tuning
- Troubleshooting guide

#### Testing
- Unit tests for PaymentService and OrderService
- Integration tests with EmbeddedKafka
- Test configuration profile with optimized settings
- Test coverage for validation scenarios

### Configuration

- `application.properties` - Development configuration
- `application-prod.properties` - Production configuration with SSL/SASL
- `application-test.properties` - Test configuration
- Security profiles for different deployment environments
- Environment variable injection for sensitive data

### Dependencies

- Spring Boot 4.0.0
- Spring Kafka 3.x
- Apache Kafka Client 3.8.0
- Micrometer Prometheus
- Micrometer Tracing with Brave
- Lombok
- Jackson for JSON processing
- JUnit 5 & Mockito for testing
- TestContainers for integration testing

## Migration Guide

### From Spring Boot 3.x to 4.x

If migrating from Spring Boot 3.x:

1. Update Java to 21+
2. Update Spring Boot version to 4.0.0
3. Update Spring Kafka to 3.x
4. Update all dependencies using `mvn dependency:tree`
5. Review deprecated features and update code accordingly
6. Test thoroughly with integration tests

## Known Issues

None at this time.

## Future Enhancements

- [ ] Support for Avro/Protobuf schemas
- [ ] Schema Registry integration
- [ ] Batch listener support for bulk operations
- [ ] Custom metrics for business KPIs
- [ ] Circuit breaker pattern for downstream services
- [ ] Request/response pattern support
- [ ] Transaction support for transactional producers
- [ ] Metrics export to CloudWatch/DataDog
- [ ] Helm charts for Kubernetes deployment
- [ ] Multi-region consumer setup

## Support

For issues, questions, or contributions, please visit:
https://github.com/satishsinghpbh/kafka-consumer
