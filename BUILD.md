# Maven Build Instructions for Kafka Consumer

## Prerequisites

- Java 21 or higher
- Maven 3.8.1 or higher
- Git

## Build Commands

### Clean Build
```bash
mvn clean package
```

### Build without Tests
```bash
mvn clean package -DskipTests
```

### Run Tests Only
```bash
mvn test
```

### Run Integration Tests
```bash
mvn verify
```

### Build with Specific Profile
```bash
# Development
mvn clean package -Pdev

# Production
mvn clean package -Pprod

# Test
mvn clean package -Ptest
```

### Skip Checkstyle/Spotbugs (if configured)
```bash
mvn clean package -DskipTests
```

## Running the Application

### From Maven
```bash
mvn spring-boot:run
```

### From Built JAR
```bash
java -jar target/kafka-consumer-1.0.0.jar
```

### With Environment Variables
```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export SPRING_PROFILES_ACTIVE=prod
java -jar target/kafka-consumer-1.0.0.jar
```

## Docker Build

### Build Docker Image
```bash
docker build -t kafka-consumer:1.0.0 .
```

### Run Docker Container
```bash
docker run -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e SPRING_PROFILES_ACTIVE=prod \
  kafka-consumer:1.0.0
```

## Docker Compose

### Start All Services
```bash
docker-compose up -d
```

### View Logs
```bash
docker-compose logs -f kafka-consumer
```

### Stop All Services
```bash
docker-compose down
```

## Development Workflow

1. **Clone Repository**
```bash
git clone https://github.com/satishsinghpbh/kafka-consumer.git
cd kafka-consumer
```

2. **Build Project**
```bash
mvn clean install
```

3. **Start Kafka** (if not using Docker)
```bash
docker-compose up -d kafka zookeeper
```

4. **Run Application**
```bash
mvn spring-boot:run
```

5. **Access Application**
- Application: http://localhost:8080
- Health: http://localhost:8080/actuator/health
- Metrics: http://localhost:8080/actuator/prometheus

## Troubleshooting

### Maven Build Failures

**Issue: "Cannot find symbol"**
```bash
# Clean Maven cache
rm -rf ~/.m2/repository
mvn clean install
```

**Issue: "Compilation failure"**
```bash
# Ensure Java 21 is used
java -version
# Should show Java 21

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java21
```

**Issue: "Tests timeout"**
```bash
# Skip tests during build
mvn clean package -DskipTests

# Or increase timeout
mvn test -DargLine="-XX:+UseG1GC"
```

### Docker Issues

**Issue: "Cannot connect to Kafka"**
```bash
# Ensure Kafka is running
docker ps | grep kafka

# Check Kafka logs
docker logs kafka
```

**Issue: "Port already in use"**
```bash
# Find process using port 9092
lsof -i :9092

# Kill process (if needed)
kill -9 <PID>
```

## Performance Tips

### Build Performance
```bash
# Parallel builds
mvn clean package -T 1C

# Skip tests for faster builds
mvn clean package -DskipTests

# Use offline mode if dependencies are cached
mvn clean package -o
```

### Runtime Performance
```bash
# Increase JVM memory
java -Xmx2g -Xms1g -jar kafka-consumer-1.0.0.jar

# Enable G1GC for large heaps
java -XX:+UseG1GC -Xmx2g -jar kafka-consumer-1.0.0.jar
```

## IDE Setup

### IntelliJ IDEA
1. Open project root
2. Maven will auto-detect pom.xml
3. Right-click pom.xml → Maven → Reload
4. Run/Debug configurations will be auto-discovered

### VS Code
1. Install "Extension Pack for Java"
2. Install "Maven for Java"
3. Open project root
4. VS Code will auto-configure Maven

### Eclipse
1. Import → Existing Maven Projects
2. Select project root
3. Eclipse will auto-configure

## Continuous Integration

### GitHub Actions Example
```yaml
name: Build and Test

on: [push, pull_request]

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-java@v3
        with:
          java-version: '21'
          distribution: 'temurin'
      - run: mvn clean verify
```

## Release Process

### Build Release JAR
```bash
mvn clean package -DskipTests
```

### Build and Push Docker Image
```bash
mvn clean package
docker build -t registry.example.com/kafka-consumer:1.0.0 .
docker push registry.example.com/kafka-consumer:1.0.0
```

### Deploy to Kubernetes
```bash
kubectl apply -f k8s-deployment.yaml
```
