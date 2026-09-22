# ResilientFlow AI

AI-assisted Saga orchestration for distributed order workflows. The project combines Spring Boot microservices, Kafka event choreography, MCP service tools, RAG-based incident diagnosis, and policy-controlled compensation.

## Architecture

```text
Order -> Orchestrator -> Product Validation -> Payment -> Inventory -> Completed
                  \-> Kafka rollback topics on failure

notify-ending (FAIL)
  -> OperationsAgent: Redis cache or pgvector RAG
  -> saga-remediation-dlq-retry
  -> validated inventory/payment compensation or human escalation
```

| Service | Port | Responsibility | Storage |
|---|---:|---|---|
| order-service | 3000 | Order API and event producer | MongoDB |
| product-validation-service | 8090 | Product validation | PostgreSQL |
| payment-service | 8091 | Payment, fraud, and refunds | PostgreSQL |
| inventory-service | 8092 | Stock reservation and release | PostgreSQL |
| orchestrator-service | 8050 | Saga state machine and dynamic plans | Redis |
| ai-saga-agent | 8099 | AI agents, RAG, remediation | PostgreSQL/pgvector, Redis |

## AI capabilities

- **OperationsAgent:** Diagnoses failed sagas using LangChain4j, Gemini/Ollama, pgvector, and historical incident context.
- **SagaComposerAgent:** Generates customer-profile-specific saga plans and stores them in Redis.
- **DataAnalystAgent:** Answers operational questions through MCP tools exposed by the microservices.
- **Semantic cache:** Hashes normalized failure traces and query embeddings in Redis. Exact cache hits bypass embedding, vector search, and LLM calls; the default cache TTL is 24 hours.
- **Compensation engine:** Publishes versioned JSON remediation commands to Kafka, validates allowlisted actions, and escalates unsafe or failed actions.

Automated compensation is bounded by failed-rollback evidence, a maximum of three actions, a R$500 refund limit, and 30-day Redis idempotency keys. Inventory release and payment refunds are duplicate-safe.

## Technology

Java 21 · Spring Boot · Apache Kafka · LangChain4j · Google Gemini · Ollama · MCP · PostgreSQL · pgvector · MongoDB · Redis 7 · JPA/Hibernate · Docker Compose · Gradle · Prometheus · Grafana

## Quick start

### Prerequisites

- Java 21+
- Docker and Docker Compose
- Ollama with `nomic-embed-text`
- Gemini API key (unless using a local chat model)

### Start infrastructure

```bash
docker compose up -d order-db product-db payment-db inventory-db vectors-db redis kafka redpanda-console
```

### Build services

```bash
./build-all.sh
# or: ./build-all.sh --with-tests
```

### Run the AI agent

```bash
cd ai-saga-agent
GEMINI_API_KEY=your-key ./gradlew bootRun
```

The remaining services can be started with `./gradlew bootRun` from their respective directories. After all JARs are built, `docker compose up -d` can run all six applications, including `ai-saga-agent`.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `KAFKA_BROKER` | `localhost:9092` | Kafka bootstrap server |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Redis connection |
| `DIAGNOSTIC_CACHE_TTL` | `PT24H` | Semantic diagnostic cache TTL |
| `VECTORS_DB_HOST` / `VECTORS_DB_PORT` | `localhost` / `5435` | pgvector database |
| `GEMINI_API_KEY` | — | Gemini access key |
| `OLLAMA_BASE_URL` | `http://localhost:11434` | Local model endpoint |

## Project structure

```text
saga-commons/                  Shared DTOs and remediation contracts
order-service/                 Order API and MongoDB integration
product-validation-service/    Product validation service
payment-service/               Payment and refund service
inventory-service/             Inventory service
orchestrator-service/          Saga coordination and Redis planning
ai-saga-agent/                 AI agents, RAG, cache, and remediation engine
docker-compose.yml             Local infrastructure
build-all.sh                   Build and publish helper
```

## Useful endpoints

| Endpoint | Purpose |
|---|---|
| `GET http://localhost:8099/api/agent/chat?question=...` | Ask the DataAnalystAgent |
| `GET http://localhost:<port>/sse` | Connect to a service MCP server |
| `http://localhost:8081` | Redpanda Kafka console |
| `http://localhost:9090` | Prometheus |
| `http://localhost:3001` | Grafana |
