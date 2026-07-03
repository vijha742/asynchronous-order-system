# EVENT-DRIVEN ORDER PROCESSING SYSTEM

## Project Reference & Engineering Playbook

*Apache Kafka · Spring Boot · PostgreSQL · Docker*

|               |                                                 |
| ------------- | ----------------------------------------------- |
| **Owner**     | Vikas                                           |
| **Domain**    | Enterprise Backend / Distributed Systems        |
| **Target**    | Java Backend / Backend SDE                      |
| **Timeline**  | 6 weeks (Phase 1 → Deployed MVP)                |

---

## 1. Project Overview

This project simulates a real-world e-commerce order lifecycle using event-driven, asynchronous communication between microservices. It is not a tutorial clone — the goal is to make deliberate architectural decisions, encounter real failure scenarios, and be able to articulate every tradeoff in an interview.

### What you're building

Three independent microservices communicating exclusively through Kafka topics:

- **Order Service** — accepts customer orders via REST, publishes events
- **Payment Service** — consumes order events, processes (simulated) payments, emits outcomes
- **Inventory Service** — listens for payment confirmations, reserves/releases stock

The architecture deliberately avoids synchronous HTTP between services. Every inter-service call is a Kafka event. This forces you to handle partial failures, retries, and eventual consistency — exactly the scenarios interviewers probe.

> **✓ INTERVIEW TIP**
> The killer interview question this project answers: "What happens if payment succeeds but the inventory service crashes before it can reserve stock?" You will have a real, implemented answer.

### Why this project, specifically

| Interview topic                 | How this project covers it                                                    |
| ------------------------------- | ----------------------------------------------------------------------------- |
| Eventual consistency            | Payment and inventory states converge without synchronous coupling            |
| Idempotency                     | Duplicate Kafka messages must not create duplicate orders/charges             |
| Dead-letter queues              | Failed messages park in DLQ for inspection/replay                             |
| Distributed failure handling    | Each service fails independently; system recovers gracefully                  |
| Schema evolution                | Producers and consumers evolve independently via versioned events             |
| Outbox pattern                  | Atomically write DB row + event without a 2PC transaction                     |

---

## 2. Tech Stack & Tooling

| Tool                       | Version (suggested)  | Purpose                                                       |
| -------------------------- | -------------------- | ------------------------------------------------------------- |
| Spring Boot                | 3.2.x                | Service framework (REST + Kafka consumer/producer)            |
| Apache Kafka               | 3.6.x (via Docker)   | Event streaming backbone                                      |
| Zookeeper / KRaft          | KRaft mode preferred | Kafka coordination (no separate ZK needed in KRaft)           |
| PostgreSQL                 | 15.x                 | Persistent state for each service (separate schemas)          |
| Spring Kafka               | bundled with Boot    | KafkaTemplate, @KafkaListener, error handlers                 |
| Docker Compose             | latest               | Spin up Kafka + 3 Postgres instances locally                  |
| Avro + Schema Registry     | Confluent OSS        | Typed event schemas with evolution support (Phase 2)          |
| Prometheus + Grafana       | via Docker           | Consumer lag, throughput, error rate dashboards               |
| Testcontainers             | 1.19.x               | Integration tests with real Kafka + Postgres                  |
| Micrometer                 | bundled              | JVM + Kafka metrics exposed to Prometheus                     |

> **ℹ NOTE**
> Each microservice has its own Postgres schema/database. They never share a database table. If a service needs data from another service, it reads its own local projection — updated via Kafka events.

---

## 3. System Architecture

### Event flow

Every state transition produces an event. Services are decoupled — they only know about Kafka topics, not each other's APIs.

| Producer          | Topic (event type)         | Consumer(s)                                                 |
| ----------------- | -------------------------- | ----------------------------------------------------------- |
| Order Service     | `order.created`            | Payment Service                                             |
| Payment Service   | `payment.processed`        | Order Service, Inventory Service                            |
| Payment Service   | `payment.failed`           | Order Service                                               |
| Inventory Service | `inventory.reserved`       | Order Service                                               |
| Inventory Service | `inventory.insufficient`   | Order Service, Payment Service (trigger refund)             |
| Any service       | `<topic>.DLT`              | Manual inspection / replay job                              |

### Order state machine

The Order Service owns the canonical order status. It updates state by listening to events from downstream services.

| Order status          | Triggered by event             | Next possible statuses                          |
| --------------------- | ------------------------------ | ----------------------------------------------- |
| PENDING               | POST /orders (REST)            | PAYMENT_PROCESSING                              |
| PAYMENT_PROCESSING    | `order.created` consumed       | PAYMENT_CONFIRMED, PAYMENT_FAILED               |
| PAYMENT_CONFIRMED     | `payment.processed` received   | INVENTORY_RESERVED, INVENTORY_FAILED            |
| PAYMENT_FAILED        | `payment.failed` received      | CANCELLED                                       |
| INVENTORY_RESERVED    | `inventory.reserved` received  | CONFIRMED (terminal)                            |
| INVENTORY_FAILED      | `inventory.insufficient`       | Triggers refund → REFUND_PENDING                |
| CONFIRMED             | All steps succeeded            | — (terminal)                                    |
| CANCELLED             | Payment or inventory failure   | — (terminal)                                    |

### Critical design decisions (know these cold)

**Idempotency** — Every Kafka consumer must be idempotent — processing the same message twice must produce the same result as processing it once. Implement this by storing a `processed_event_ids` table per service and checking before acting.

> **✓ INTERVIEW TIP**
> Interview question: "Kafka guarantees at-least-once delivery. How do you ensure duplicate messages don't double-charge customers?" Answer: idempotency key stored in DB, checked before processing.

**Outbox pattern** — Never publish to Kafka inside a DB transaction. Instead, write the event to an outbox table in the same transaction as the business record, then a separate poller publishes it. This gives you atomic write + guaranteed event delivery.

**Dead-letter topics** — When a consumer throws an exception after N retries, Spring Kafka routes the message to a Dead Letter Topic (DLT). Build a simple admin endpoint or scheduled job to inspect and replay DLT messages. This is a production concern that most student projects skip — it will impress interviewers.

**Saga pattern (compensating transactions)** — If inventory reservation fails after payment succeeded, you must trigger a refund. This is the choreography-based Saga pattern: each service emits events that trigger compensating actions in upstream services. No central orchestrator.

---

## 4. Repository Structure

Use a monorepo with one Maven/Gradle multi-module project. This keeps shared DTOs in one place and simplifies Docker Compose configuration.

```
event-order-system/
├── order-service/         REST API, order entity, outbox poller, state machine
├── payment-service/       Payment logic (simulated), consumer, producer
├── inventory-service/     Stock management, reservation logic, consumer
├── shared-events/         Avro schemas or Java POJOs for all event types
├── docker/                docker-compose.yml, Kafka config, Postgres init scripts
├── monitoring/            Prometheus config, Grafana dashboards JSON
└── docs/                  Architecture diagrams, ADRs (Architecture Decision Records)
```

> **ℹ NOTE**
> Write at least two ADRs (Architecture Decision Records) — one for "Why Kafka over REST between services" and one for "Why choreography over orchestration". ADRs show engineering maturity that frameworks cannot teach.

---

## 5. Build Timeline (6 Weeks)

Each phase has a clear deliverable. Do not start the next phase until the current one is working end-to-end. A working Week 2 is better than a broken Week 6.

| Week | Phase                  | Deliverable                                                                                   | Definition of done                                                           |
| ---- | ---------------------- | --------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| 1    | Foundation             | Docker Compose up with Kafka (KRaft) + 3 Postgres instances. Order Service REST endpoint.     | POST /orders returns 201 with order ID. Order row in DB.                     |
| 2    | First event flow       | Order Service publishes `order.created`. Payment Service consumes, simulates payment.         | End-to-end: POST /orders → Kafka → Payment Svc logs 'payment processed'.     |
| 3    | Inventory + state machine | Inventory Service consumes `payment.processed`, reserves stock. Order Service listens & transitions. | GET /orders/{id} reflects current state. Full happy path confirmed.      |
| 4    | Failure scenarios      | Idempotency keys, DLT handling, refund saga (inventory failure → Payment Svc refund).         | Duplicate messages don't double-process. Failed inventory triggers refund.   |
| 5    | Observability + outbox | Outbox pattern on Order Service. Prometheus + Grafana dashboards. Testcontainers tests.       | Outbox poller publishes reliably. Grafana shows consumer lag.                |
| 6    | Deploy + polish        | Deploy to VPS (Hetzner / Railway) or Render. README with architecture diagram. Demo video.    | Public URL works. README has architecture diagram and local setup.           |

> **⚠ WATCH OUT**
> Week 4 (failure scenarios) is the most important week. Anyone can build the happy path. Interviewers test edge cases. Do not skip or rush Week 4.

---

## 6. Detailed Task Checklist

### Week 1 — Foundation

1. Create Maven multi-module project with parent POM
2. Write docker-compose.yml: Kafka (KRaft), 3 Postgres instances
3. Verify Kafka is reachable: produce/consume test message via CLI
4. Create Order entity + OrderStatus enum + OrderRepository
5. POST /orders endpoint — validates input, persists PENDING order
6. GET /orders/{id} endpoint — returns current status
7. Basic error handling: 400 for bad input, 404 for missing order

### Week 2 — First Event Flow

8. Define OrderCreatedEvent POJO in shared-events module
9. Configure KafkaTemplate in Order Service, publish on order creation
10. Create Payment Service: @KafkaListener on `order.created` topic
11. Simulate payment: random 80% success / 20% failure
12. Publish PaymentProcessedEvent or PaymentFailedEvent
13. Add consumer group ID and verify offset commits
14. Test: POST order → verify Payment Service logs correct outcome

### Week 3 — Inventory + State Machine

15. Create Inventory Service with stock table (productId, quantity)
16. @KafkaListener on `payment.processed` → reserve stock
17. Publish InventoryReservedEvent or InventoryInsufficientEvent
18. Order Service listens to all 4 event types, updates OrderStatus
19. Implement optimistic locking on stock reservation (no oversell)
20. Verify full happy path: PENDING → CONFIRMED
21. Write manual test script that fires 10 concurrent orders

### Week 4 — Failure Scenarios (Do not skip)

22. Add `processed_event_ids` table in each service DB
23. Check idempotency key before processing each message
24. Test: publish same message twice → verify single processing
25. Configure Spring Kafka retry: 3 attempts with exponential backoff
26. Configure DeadLetterPublishingRecoverer for DLT routing
27. Build /admin/dlq endpoint to list and replay DLT messages
28. Implement refund saga: InventoryInsufficientEvent → Payment Svc refund
29. Test full failure saga: PENDING → PAYMENT_CONFIRMED → INVENTORY_FAILED → REFUND → CANCELLED

### Week 5 — Observability + Outbox

30. Add Micrometer dependency, expose /actuator/prometheus endpoint
31. Add Prometheus scrape config for all 3 services
32. Import Grafana dashboard: consumer lag per topic, error rates, throughput
33. Implement outbox table in Order Service DB
34. Build scheduled OutboxPoller (every 500ms) — publishes unpublished rows
35. Add Testcontainers: integration test for full order→payment→inventory flow
36. Add Testcontainers: integration test for the refund saga

### Week 6 — Deploy + Polish

37. Choose deployment target: Hetzner CX11 (€3.79/mo) or Railway
38. Write production docker-compose.override.yml with resource limits
39. Set up GitHub Actions: build → test → push Docker image
40. Deploy and verify public endpoint responds correctly
41. Draw architecture diagram (use Excalidraw or draw.io), export as PNG
42. Write README: what it does, how to run locally, architecture diagram, design decisions
43. Record 2-minute Loom demo walking through a full order flow
44. Write 2 ADRs: Kafka vs REST, choreography vs orchestration

---

## 7. Interview Preparation

For every question below, you should be able to answer from your actual implementation — not from theory. Your answer should include: what you did, why you did it that way, and what the tradeoff was.

### Must-know questions (will be asked)

| Question                                                                     | Key points in your answer                                                                                                |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| What happens if payment succeeds but inventory service is down?              | Explain the saga pattern + compensating transaction. Order stays in PAYMENT_CONFIRMED until inventory recovers or timeout triggers refund. |
| How do you ensure no duplicate orders if Kafka delivers a message twice?     | Idempotency key stored in `processed_event_ids`. Upsert with conflict-do-nothing. At-least-once + idempotency = effectively-once. |
| Why Kafka instead of synchronous REST between services?                      | Decoupling, backpressure, durability, replay. REST creates tight coupling and cascading failures.                        |
| How do you guarantee a DB write and Kafka publish happen atomically?         | Outbox pattern: write event to outbox table in same transaction. Poller publishes. No distributed 2PC needed.            |
| What is consumer lag and why does it matter?                                 | Difference between latest offset and committed offset. High lag = consumers falling behind. Grafana dashboard shows this. |
| How would you scale this to 10x order volume?                                | Add Kafka partitions. Add consumer instances (one per partition max). Partition by orderId for ordering guarantees.       |
| What is the difference between at-least-once and exactly-once delivery?      | At-least-once: message may duplicate, consumer must be idempotent. Exactly-once: Kafka transactions + idempotent producer. You chose at-least-once + idempotent consumers — explain why. |

### How to walk through the project in 5 minutes

45. Start with the problem: "I wanted to simulate how a real e-commerce order flows across services without tight coupling."
46. Draw the flow on a whiteboard (or verbally): Order → Kafka → Payment → Kafka → Inventory.
47. Go straight to the hard part: "The interesting problem is: what if one service fails mid-flow?"
48. Explain your saga implementation and idempotency solution with specific code references.
49. End with observability: "I added consumer lag tracking in Grafana so you can see when a service falls behind in real time."

> **✓ INTERVIEW TIP**
> Never start with the tech stack. Start with the problem you were solving. Interviewers want to know you made deliberate decisions, not that you know what Kafka is.

---

## 8. Key Resources

| Resource                                               | What to read/watch                                                                                |
| ------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| Confluent Kafka documentation                          | Producer configs: acks, retries, idempotence. Consumer configs: auto.offset.reset, max.poll.records. |
| Spring Kafka reference docs                            | KafkaTemplate, @KafkaListener, DefaultErrorHandler, DeadLetterPublishingRecoverer.                |
| "Designing Data-Intensive Applications" — Kleppmann, Ch. 11 | Stream processing and event logs. The canonical reference for Kafka architecture reasoning.       |
| Microservices.io — Saga pattern                        | Choreography vs orchestration. Use this URL in your README.                                       |
| Microservices.io — Outbox pattern                      | Exact implementation reference for transactional outbox.                                          |
| Testcontainers Spring Boot guide                       | Official guide for Kafka + Postgres integration tests.                                            |
| Baeldung — Spring Kafka error handling                 | Retry, backoff, and DLT configuration with code examples.                                         |

---

## 9. Quick Reference: Key Kafka Configs

These config values will come up in interviews. Know what each one does and why you chose it.

| Config key                    | Recommended value          | Why it matters                                                                  |
| ----------------------------- | -------------------------- | ------------------------------------------------------------------------------- |
| `acks` (producer)             | `all`                      | All in-sync replicas must acknowledge — strongest durability guarantee.          |
| `enable.idempotence` (producer) | `true`                   | Prevents duplicate messages from producer retries.                              |
| `retries` (producer)          | `Integer.MAX_VALUE`        | Retry indefinitely with idempotence enabled.                                    |
| `auto.offset.reset` (consumer)| `earliest`                 | On new consumer group, read from beginning. Safer for dev.                      |
| `enable.auto.commit` (consumer)| `false`                   | Manual offset commit — only commit after successful processing.                 |
| `max.poll.records` (consumer) | `10–50`                    | Limit batch size to avoid processing timeout.                                   |
| `group.id` (consumer)         | Per-service, e.g. `payment-service-group` | Each service has its own consumer group — all get every message.                |

---

*Ship the hard parts. Interviewers remember the edge cases you solved.*