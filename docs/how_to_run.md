## Prerequisites
- Docker & Docker Compose

## 1. Obtain the Docker Image
Pull the pre-built image from your registry (replace `<image-name>` with your actual image name, e.g., `ghcr.io/yourorg/yappa-rt:latest`):
```
docker pull shan117/yappa-rt:latest
```

## 2. Set Up Environment Variables
Copy the example environment file and edit as needed:
```
cp .env.example .env
# Edit .env with your preferred settings
```.env
JWT_SECRET="YOUR_JWT_SECRET_HERE"
REDIS_URL=
KAFKA_BROKERS=
DATABASE_URL=
```

## 3. Update docker-compose.yml
Edit the `docker-compose.yml` to use the image instead of building from source. Example:
```yaml
services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      JWT_SECRET: ${JWT_SECRET:-"supersecretkey"}
      REDIS_URL: redis://redis:6379
      KAFKA_BROKERS: kafka:9092
      DATABASE_URL: postgres://realtime:realtime@postgres:5432/realtime
      PORT: "8080"
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
    restart: unless-stopped

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: realtime
      POSTGRES_PASSWORD: realtime
      POSTGRES_DB: realtime
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U realtime"]
      interval: 5s
      timeout: 3s
      retries: 5

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
    volumes:
      - kafkadata:/bitnami/kafka
    healthcheck:
      test: ["CMD-SHELL", "kafka-broker-api-versions.sh --bootstrap-server localhost:9092"]
      interval: 10s
      timeout: 5s
      retries: 10

volumes:
  pgdata:
  redisdata:
  kafkadata:

```
Remove or comment out the `build: .` line in the `app` service.

## 4. Start All Services
```
docker-compose up -d
```

The app will be available at http://localhost:8080

## 5. View Logs
To see logs for the app service:
```
docker-compose logs -f app
```

## 6. Stopping Services
To stop and remove all containers:
```
docker-compose down
```

---

