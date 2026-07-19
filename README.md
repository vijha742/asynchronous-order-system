# Event-Driven Order Processing System

A distributed, event-driven e-commerce order processing system built with Apache Kafka, Spring Boot 4, and PostgreSQL. Three independent microservices communicate exclusively through Kafka topics — no synchronous HTTP between services.

______________________________________________________________________

## What This Is

This project simulates a real-world e-commerce order lifecycle: a customer places an order, payment is processed, and inventory is reserved — all asynchronously, across independent services. Each service owns its own database and publishes/consumes events from Kafka topics.

The goal is not a tutorial clone. Every architectural decision here was made deliberately: why Kafka over REST, why choreography over orchestration, why at-least-once delivery with idempotent consumers instead of exactly-once semantics. These are the tradeoffs that come up in system design interviews, and this project provides real implementations to back them up.

______________________________________________________________________

## Architecture

Three services, one message bus, no direct service-to-service HTTP calls.

```
POST /orders
     |
     v
Order Service  ──(order.created)──────────────────►  Payment Service
     ^                                                      |
     |                                            (payment.processed)
     |                                            (payment.failed)
     |                                                      |
     |◄────────────────────────────────────────────────────┤
     |                                                      |
     |                                                      v
     |                                             Inventory Service
     |                                                      |
     |◄──────────(inventory.reserved)──────────────────────┤
     |◄──────────(inventory.insufficient)──────────────────┘
```

### Kafka Topics

| Producer | Topic | Consumer(s) |
|-------------------|--------------------------|------------------------------------------|
| Order Service | `order.created` | Payment Service |
| Payment Service | `payment.processed` | Order Service, Inventory Service |
| Payment Service | `payment.failed` | Order Service |
| Inventory Service | `inventory.reserved` | Order Service |
| Inventory Service | `inventory.insufficient` | Order Service, Payment Service |
| Any service | `<topic>.DLT` | Dead Letter Topic — inspect and replay |

### Order State Machine

The Order Service owns the canonical order status. It transitions state by consuming events from downstream services.

```
PENDING
  └─► PAYMENT_PROCESSING
        ├─► PAYMENT_CONFIRMED
        │     ├─► INVENTORY_RESERVED ──► CONFIRMED  (terminal)
        │     └─► INVENTORY_FAILED ────► CANCELLED  (terminal)
        └─► PAYMENT_FAILED ──────────► CANCELLED    (terminal)
```

State transitions are strictly validated. An illegal transition attempt (e.g., jumping from `CONFIRMED` back to `PENDING`) is rejected at the domain level via `OrderStatus.canTransitionTo()`.

______________________________________________________________________

## Key Engineering Decisions

### Idempotency

Kafka guarantees at-least-once delivery. The same message can arrive twice. To prevent double-processing (e.g., double-charging a customer), each service maintains a `processed_event_ids` table. Before acting on any event, the consumer checks whether that event ID has already been processed. If it has, the message is silently skipped. This makes each consumer effectively idempotent.

### Outbox Pattern

Publishing a Kafka event inside a database transaction is unsafe — the transaction can commit but the Kafka publish can fail, leaving the system in an inconsistent state. The outbox pattern solves this: the event is written to an `outbox` table in the same transaction as the business record. A scheduled poller reads unpublished rows and publishes them to Kafka. Atomicity without distributed 2PC.

### Saga Pattern (Choreography)

If inventory reservation fails after payment was already confirmed, the system must issue a refund. This is handled via a choreography-based saga: the Inventory Service emits `inventory.insufficient`, the Payment Service consumes it and issues a refund, and the Order Service marks the order as `CANCELLED`. No central orchestrator. Each service reacts to events and emits its own.

### Dead Letter Topics

When a consumer throws an exception after N retries (3 attempts with exponential backoff), Spring Kafka routes the message to a Dead Letter Topic (`<topic>.DLT`). An `/admin/dlq` endpoint allows inspection and manual replay of failed messages.

______________________________________________________________________

## Tech Stack

| Component | Technology |
|--------------------|-----------------------------------|
| Services | Spring Boot 4.0.1, Java 21 |
| Messaging | Apache Kafka (KRaft mode) |
| Persistence | PostgreSQL 15 (one DB per service)|
| Metrics | Micrometer + Prometheus |
| Dashboards | Grafana |
| Integration Tests | Testcontainers (Kafka + Postgres) |
| Build | Maven multi-module |

Kafka runs in KRaft mode — no Zookeeper dependency. Each microservice has its own isolated PostgreSQL database. Services never share a table.

______________________________________________________________________

## Repository Structure

```
event-driven-ordering-system/
├── order-service/          REST API, order entity, outbox poller, state machine
├── payment-service/        Payment logic (simulated), consumer, producer
├── inventory-service/      Stock management, reservation logic, consumer
├── shared-events/          Shared event POJOs consumed across services
├── monitoring/             Prometheus config, Grafana dashboard JSON
├── docs/                   Project reference, architecture decisions, case studies
├── docker-compose.yaml     Full local stack: Kafka, 3x Postgres, Prometheus, Grafana
└── order-testing.sh        Fires 10 concurrent orders for manual load testing
```

______________________________________________________________________

## Running Locally

**Prerequisites:** Docker, Docker Compose, Java 21, Maven

### 1. Start the infrastructure

```bash
docker compose up -d
```

This starts:

- Kafka (KRaft, port 9092)
- PostgreSQL for Order Service (port 5433)
- PostgreSQL for Payment Service (port 5434)
- PostgreSQL for Inventory Service (port 5435)
- Prometheus (port 9090)
- Grafana (port 3000)

### 2. Build the project

```bash
mvn clean install -DskipTests
```

### 3. Start each service

Run each in a separate terminal:

```bash
# Order Service
cd order-service && mvn spring-boot:run

# Payment Service
cd payment-service && mvn spring-boot:run

# Inventory Service
cd inventory-service && mvn spring-boot:run
```

### 4. Place an order

```bash
curl -s -X POST "http://localhost:8080/api/v1/orders?productId=1&quantity=5"
```

### 5. Poll order status

```bash
curl http://localhost:8080/api/v1/orders/{orderId}
```

Watch the status transition from `PENDING` through to `CONFIRMED` (or `CANCELLED` on a simulated payment failure).

### 6. Fire concurrent orders (load test)

```bash
chmod +x order-testing.sh && ./order-testing.sh
```

Fires 10 concurrent orders with random product IDs and quantities.

______________________________________________________________________

## Observability

| Endpoint | What it shows |
|---------------------------------|------------------------------------------------------|
| `http://localhost:9090` | Prometheus — raw metrics, scrape targets |
| `http://localhost:3000` | Grafana (admin/admin) — pre-built dashboard |
| `http://localhost:8080/actuator/prometheus` | Order Service metrics endpoint |

The Grafana dashboard tracks:

- Kafka consumer lag per topic
- Event throughput (events/sec)
- Error rates and retry counts

Consumer lag is the most important metric in an event-driven system. If a service falls behind in processing, lag grows. The dashboard makes this visible in real time.

______________________________________________________________________

## Running Tests

Integration tests use Testcontainers — they spin up real Kafka and PostgreSQL containers automatically. No manual setup required.

```bash
mvn test
```

Two integration test suites exist:

- **HappyPathIntegrationTest** — validates the full `PENDING → CONFIRMED` flow
- **RefundSagaIntegrationTest** — validates the `PAYMENT_CONFIRMED → INVENTORY_FAILED → CANCELLED` failure flow, including the compensating refund transaction

Tests are written against Spring Boot 4.0's `RestTestClient` (the unified replacement for `TestRestTemplate`).

______________________________________________________________________

## Producer Configuration

These values are set deliberately and matter for durability and correctness:

| Config | Value | Reason |
|---------------------------|---------------------|-------------------------------------------------------------|
| `acks` | `all` | All in-sync replicas must acknowledge — strongest durability |
| `enable.idempotence` | `true` | Prevents duplicate messages from producer retries |
| `retries` | `Integer.MAX_VALUE` | Retry indefinitely when idempotence is enabled |
| `auto.offset.reset` | `earliest` | New consumer group reads from the beginning |
| `enable.auto.commit` | `false` | Offsets committed only after successful processing |
| `max.poll.records` | `10–50` | Limits batch size to avoid poll timeout |
| `group.id` | Per-service | Each service gets all messages independently |

______________________________________________________________________

## Failure Scenarios Covered

| Scenario | Handling |
|-------------------------------------------------|------------------------------------------------------------------------|
| Duplicate Kafka message delivered | Idempotency check against `processed_event_ids` table |
| Consumer throws exception | 3 retries with exponential backoff, then routed to DLT |
| Payment succeeds, inventory service is down | Order stays in `PAYMENT_CONFIRMED`; inventory processes when it recovers|
| Inventory fails after payment | Refund saga: `inventory.insufficient` → Payment Service refunds → `CANCELLED` |
| Kafka publish fails after DB write | Outbox poller retries; event is not lost |

______________________________________________________________________

## Design Decisions (ADRs)

**Why Kafka instead of REST between services?**

Synchronous REST creates tight coupling. If the Payment Service is slow, the Order Service blocks. If the Inventory Service is down, calls fail immediately. Kafka decouples producers from consumers — the Order Service publishes an event and moves on. Consumers process when they are ready. Kafka also provides replay: if a consumer crashes, it re-reads from its last committed offset. REST calls cannot be replayed.

**Why choreography instead of orchestration?**

An orchestrator (e.g., a Saga orchestrator service) becomes a single point of failure and a central bottleneck. Choreography distributes responsibility: each service knows what events to emit and how to react. The tradeoff is that the overall flow is harder to visualize — it is spread across service boundaries rather than centralized in one place. For this system's scope, choreography keeps each service independently deployable without depending on a central coordinator.

______________________________________________________________________

## Project Status

| Phase | Status |
|---------------------------|-------------|
| Infrastructure + REST API | Complete |
| First event flow | Complete |
| Inventory + state machine | Complete |
| Failure scenarios + DLT | Complete |
| Outbox + observability | Complete |
| Integration tests | Complete |
| Deployment | In progress |
