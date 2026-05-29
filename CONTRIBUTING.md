# Contributing Guide

Thank you for your interest in contributing to the Kafka Consumer project! This guide will help you get started.

## Code of Conduct

- Be respectful and inclusive
- Provide constructive feedback
- Report security vulnerabilities privately

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Commit with meaningful messages: `git commit -am 'Add feature description'`
6. Push to your branch: `git push origin feature/your-feature`
7. Create a Pull Request

## Development Setup

```bash
# Clone repository
git clone https://github.com/satishsinghpbh/kafka-consumer.git
cd kafka-consumer

# Build project
mvn clean install

# Start dependencies
docker-compose up -d

# Run tests
mvn test

# Run application
mvn spring-boot:run
```

## Code Standards

### Java Code Style

- Use Java 21+ features when appropriate
- Follow Google Java Style Guide
- Maximum line length: 120 characters
- Use meaningful variable names
- Add Javadoc for public methods

Example:
```java
/**
 * Process payment event with validation and error handling
 *
 * @param payment the payment event to process
 * @throws PaymentProcessingException if processing fails
 */
public void processPayment(PaymentEvent payment) {
    // Implementation
}
```

### Testing Requirements

- Minimum 70% code coverage
- Write unit tests for business logic
- Write integration tests for Kafka interactions
- Use descriptive test method names
- Test both success and failure scenarios

Example:
```java
@Test
void testProcessPaymentWithValidData() {
    // Arrange
    PaymentEvent payment = createTestPayment();
    
    // Act & Assert
    assertDoesNotThrow(() -> paymentService.processPayment(payment));
}

@Test
void testProcessPaymentWithNegativeAmount() {
    // Arrange
    PaymentEvent payment = createTestPayment();
    payment.setAmount(new BigDecimal("-10.00"));
    
    // Act & Assert
    assertThrows(PaymentProcessingException.class, 
        () -> paymentService.processPayment(payment));
}
```

### Configuration

- Use environment variables for secrets
- Never hardcode credentials
- Document all configuration properties
- Provide sensible defaults

## Commit Message Guidelines

Use clear, descriptive commit messages:

```
Type: Brief description

Longer explanation if needed. Should wrap at 72 characters.

Fixes #123 (if applicable)
```

Types:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation only
- `style:` Code style changes (formatting, missing semicolons, etc.)
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `test:` Adding tests
- `chore:` Maintenance tasks

Examples:
```
feat: Add support for custom deserializers

fix: Prevent rebalance loop in static membership

docs: Update README with Kubernetes examples

test: Add integration tests for DLT handling
```

## Pull Request Process

1. **Title**: Clear, concise description of changes
2. **Description**: 
   - What problem does this solve?
   - How does it solve it?
   - Any breaking changes?
3. **Checklist**:
   - [ ] Tests added/updated
   - [ ] Documentation updated
   - [ ] No hardcoded secrets
   - [ ] Code follows style guide
   - [ ] Commit messages are clear

## Review Process

- Maintainers will review PRs within 7 days
- Address feedback promptly
- Feel free to ask questions
- Once approved, changes will be merged

## Reporting Issues

When reporting bugs:

1. **Title**: Concise description
2. **Reproduction Steps**: Exact steps to reproduce
3. **Expected Behavior**: What should happen
4. **Actual Behavior**: What actually happens
5. **Environment**: 
   - OS and version
   - Java version
   - Spring Boot version
   - Kafka version

Example:
```
Title: Consumer lag increases indefinitely with manual acknowledgment

Reproduction:
1. Start application with 1 partition topic
2. Set concurrency=2
3. Send 100 messages

Expected: Consumer processes all messages

Actual: Consumer lag increases, no messages processed

Environment:
- Ubuntu 22.04
- Java 21.0.1
- Spring Boot 4.0.0
- Kafka 3.8.0
```

## Documentation

- Update README.md for user-facing changes
- Update BUILD.md for build/deployment changes
- Add inline code comments for complex logic
- Update CHANGELOG.md with your changes

## Security

If you discover a security vulnerability:

1. **DO NOT** create a public issue
2. Email maintainer privately with details
3. Include: description, affected versions, reproduction steps
4. Allow time for patch before public disclosure

## License

By contributing, you agree that your contributions will be licensed under the MIT License.

## Questions?

- Open a discussion on GitHub
- Check existing issues and PRs
- Review project documentation

Thank you for contributing! 🙌
