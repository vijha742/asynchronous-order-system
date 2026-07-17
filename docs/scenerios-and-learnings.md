# Case Study: Migrating Integration Tests to Spring Boot 4.0 & Spring 7

This document details a real-world engineering challenge encountered when implementing integration tests with Testcontainers in the event-driven ordering system. Here I discuss **"A challenging technical problem I faced while building this project, and how did I resolve it?"**

---

## 1. The Interview Pitch (Elevator Version)

> "During the migration of our `order-service` to **Spring Boot 4.0.1** and **Spring Framework 7**, our end-to-end integration tests (which run against real PostgreSQL and Kafka Testcontainers) broke. The compiler could not locate our web testing clients, and Spring's application context failed to boot. 
>
> I had to debug classpath class resolution, analyze Spring Boot's new autoconfiguration internals for REST clients, and migrate legacy `TestRestTemplate` invocations to the newly introduced, unified `RestTestClient` API. Ultimately, I restored the test suite to a 100% green pass state while modernizing the test codebase to use Spring 7 best practices."

---

## 2. The Symptom & Error Details

When running `mvn test`, the build failed with two distinct compiler/runtime issues:

1. **Compilation Failures**:
   ```text
   [ERROR] package org.springframework.boot.resttestclient.autoconfigure does not exist
   [ERROR] cannot find symbol: class AutoConfigureRestTestClient
   ```
2. **Context Bootstrap Failure** (prior to the dependency restoration):
   ```text
   NoSuchBeanDefinitionException: No qualifying bean of type 'org.springframework.boot.test.web.client.TestRestTemplate' available
   ```

---

## 3. The Debugging Journey & Hypotheses

### Hypothesis 1: Redundant Annotation Pollution
We noticed that `@AutoConfigureTestRestTemplate` was present on both the abstract parent class (`AbstractIntegrationTest`) and the concrete subclass (`HappyPathIntegrationTest`). 
* *Debugging Action*: We removed it from the parent class to isolate subclass configurations.
* *Result*: The compilation errors persisted, meaning the issue was deeper than a duplicate annotation conflict.


### Hypothesis 2: Package & API Mismatches
Spring 7 introduces a new, unified testing client called `RestTestClient`. However, `RestTestClient` was imported as:
```java
import org.springframework.test.web.servlet.client.RestTestClient;
```
It was unclear if this class belonged to MockMvc (isolated mocking) or could support full port testing (`WebEnvironment.RANDOM_PORT`).
* *Debugging Action*: We located the actual JAR on the local filesystem and inspected its classes:
  ```bash
  jar tf ~/.m2/repository/org/springframework/spring-test/7.0.2/spring-test-7.0.2.jar | grep RestTestClient
  ```
  We confirmed that `RestTestClient` indeed lives in the `spring-test` module (Spring 7). We then analyzed Spring Boot's autoconfiguration source:
  ```bash
  javap -p org/springframework/boot/resttestclient/autoconfigure/RestTestClientAutoConfiguration.class
  ```
  This revealed that `RestTestClientAutoConfiguration` registers the bean `RestTestClient` dynamically, and when a live port exists (`RANDOM_PORT`), it configures it to target the live server automatically via `SpringBootRestTestClientBuilderCustomizer`.

---

## 4. The Root Cause

The failure was caused by two compounding issues:
1. **Missing Maven Dependency**: The `spring-boot-resttestclient` artifact had been excluded from `pom.xml`, meaning the test autoconfiguration annotations (`@AutoConfigureRestTestClient`) were missing from the compiler's classpath.
2. **Legacy Client Deprecation**: Spring Boot 4 has moved `TestRestTemplate` to maintenance mode. It is no longer registered in the application context by default under modern test autoconfigurations. To inject the modern alternative (`RestTestClient`), tests must explicitly import the Spring 7 client packages and annotate classes with `@AutoConfigureRestTestClient`.

---

## 5. The Solution

We resolved the issues systematically:

### Step 1: Restored Classpath Dependencies
We added the necessary test-scoped dependency back to `order-service/pom.xml`:
```xml
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-resttestclient</artifactId>
    <scope>test</scope>
</dependency>
```

### Step 2: Enabled Modern Auto-Configuration
We annotated both `HappyPathIntegrationTest` and `RefundSagaIntegrationTest` with `@AutoConfigureRestTestClient` to instruct Spring to bootstrap the HTTP client:
```java
@DisplayName("Happy Path Integration: order → payment → inventory → CONFIRMED")
@AutoConfigureRestTestClient
class HappyPathIntegrationTest extends AbstractIntegrationTest {
    @Autowired private RestTestClient restClient;
    // ...
}
```

### Step 3: Migrated HTTP Calls to Fluent API
We migrated all old REST calls to the modern, fluent builder style:

* **Before (Legacy Blocking):**
  ```java
  ResponseEntity<Order> createResponse = restTemplate.postForEntity("/api/v1/orders", request, Order.class);
  assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
  Order created = createResponse.getBody();
  ```

* **After (Modern Fluent):**
  ```java
  Order created = restClient.post()
          .uri("/api/v1/orders")
          .body(request)
          .exchange()
          .expectStatus().isEqualTo(HttpStatus.CREATED)
          .expectBody(Order.class)
          .returnResult()
          .getResponseBody();
  ```

---

## 6. What I learnt : 

While debugging this issue, I learnt about :
* **Checking Migration status and making consistent changes to the code**:
* **Going through dependency list to check for missing dependencies and version clashes**: In this case we were using Spring Boot 4 and Spring 7, so we had to check for the compatible dependencies.
* **AutoConfiguration updates in Spring-boot 4.0.1 for various Test classes**: Spring Boot 4.0.1 deprecated `TestRestTemplate` and introduced `RestTestClient` as a unified testing client for both isolated mocking (`MockMvc`) and real integration testing (`RestTestClient`) into a single fluent API.
* **Ecosystem Awareness**: Discuss the transition from Spring Boot 3.x to 4.x / Spring 7, explaining the rationale behind unifying isolated testing (`MockMvc`) and real integration testing (`RestTestClient`) into a single fluent API.
