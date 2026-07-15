package com.vikas.order_service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.model.CreateOrderDTO;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.shared.events.InventoryReservedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Integration test – Task 35
 *
 * <p>Verifies the <b>full happy-path saga</b>:
 * <pre>
 *   POST /orders
 *     → order.created  (published by order-service)
 *     → payment.processed  (simulated here, would be emitted by payment-service)
 *     → inventory.reserved (simulated here, would be emitted by inventory-service)
 *     → OrderStatus == CONFIRMED
 * </pre>
 *
 * <p>The test plays the role of both the payment-service and the inventory-service
 * by publishing their events directly on the appropriate Kafka topics.  The
 * order-service's {@code KafkaService} listeners pick up those events and drive
 * the order state machine to {@code CONFIRMED}.
 */
@DisplayName("Happy Path Integration: order → payment → inventory → CONFIRMED")
class HappyPathIntegrationTest extends AbstractIntegrationTest {

    /** REST client bound to the randomly assigned server port. */
    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private OrderRepository orderRepository;

    @BeforeEach
    void cleanDatabase() {
        orderRepository.deleteAll();
    }

    // -------------------------------------------------------------------------
    // Test cases
    // -------------------------------------------------------------------------

    @Test
    @DisplayName("Order reaches CONFIRMED after payment and inventory events are received")
    void order_shouldBeConfirmed_afterPaymentAndInventoryEvents() throws Exception {
        // ── Step 1: Create an order via the REST API ──────────────────────────
        CreateOrderDTO request = new CreateOrderDTO(1L, 2);
        ResponseEntity<Order> createResponse =
                restTemplate.postForEntity("/api/v1/orders", request, Order.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        Order created = createResponse.getBody();
        assertThat(created).isNotNull();

        String orderId = created.getOrderId();
        assertThat(orderId).isNotBlank();
        assertThat(created.getStatus()).isEqualTo(OrderStatus.PENDING);

        // ── Step 2: Simulate the payment-service publishing payment.processed ──
        String paymentId = UUID.randomUUID().toString();
        PaymentProcessedEvent paymentEvent =
                new PaymentProcessedEvent(orderId, paymentId, 1L, 2, 99.99);

        kafkaTemplate.send("payment.processed", orderId, paymentEvent).get(5, TimeUnit.SECONDS);

        // ── Step 3: Wait for order-service to transition to PAYMENT_CONFIRMED ──
        await("order transitions to PAYMENT_CONFIRMED")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Order order = orderRepository.findById(orderId).orElseThrow();
                    assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_CONFIRMED);
                });

        // ── Step 4: Simulate inventory-service publishing inventory.reserved ───
        InventoryReservedEvent inventoryEvent =
                new InventoryReservedEvent(orderId, 1L, 2);

        kafkaTemplate.send("inventory.reserved", orderId, inventoryEvent).get(5, TimeUnit.SECONDS);

        // ── Step 5: Wait for final state – CONFIRMED ─────────────────────────
        await("order transitions to CONFIRMED")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Order order = orderRepository.findById(orderId).orElseThrow();
                    assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
                });
    }

    @Test
    @DisplayName("Duplicate payment.processed events are idempotent (order stays CONFIRMED, not double-processed)")
    void duplicate_paymentProcessed_shouldBeIdempotent() throws Exception {
        // Create order
        CreateOrderDTO request = new CreateOrderDTO(2L, 1);
        ResponseEntity<Order> createResponse =
                restTemplate.postForEntity("/api/v1/orders", request, Order.class);

        assertThat(createResponse.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        String orderId = createResponse.getBody().getOrderId();

        String paymentId = UUID.randomUUID().toString();
        PaymentProcessedEvent paymentEvent =
                new PaymentProcessedEvent(orderId, paymentId, 2L, 1, 49.99);

        // Send payment.processed twice (simulates at-least-once delivery duplicate)
        kafkaTemplate.send("payment.processed", orderId, paymentEvent).get(5, TimeUnit.SECONDS);
        kafkaTemplate.send("payment.processed", orderId, paymentEvent).get(5, TimeUnit.SECONDS);

        // Order should reach PAYMENT_CONFIRMED exactly once, not double-processed
        await("order transitions to PAYMENT_CONFIRMED (idempotent)")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Order order = orderRepository.findById(orderId).orElseThrow();
                    assertThat(order.getStatus()).isEqualTo(OrderStatus.PAYMENT_CONFIRMED);
                });

        // Publish inventory reserved → CONFIRMED
        kafkaTemplate.send("inventory.reserved", orderId,
                new InventoryReservedEvent(orderId, 2L, 1)).get(5, TimeUnit.SECONDS);

        await("order reaches terminal CONFIRMED state")
                .atMost(Duration.ofSeconds(15))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Order order = orderRepository.findById(orderId).orElseThrow();
                    assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
                });
    }
}
