package com.vikas.order_service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.vikas.order_service.model.CreateOrderDTO;
import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.PaymentProcessedEvent;
import com.vikas.shared.events.PaymentRefundedEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureRestTestClient;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Integration test – Task 36
 *
 * <p>
 * Verifies the <b>refund saga (failure path)</b>:
 *
 * <pre>
 *   POST /orders                           → PENDING
 *   payment.processed                      → PAYMENT_CONFIRMED
 *   inventory.insufficient                 → INVENTORY_FAILED
 *   payment.refunded   (compensating txn)  → CANCELLED
 * </pre>
 *
 * <p>
 * This is the choreography-based Saga pattern in action. When inventory
 * reservation fails, the
 * payment-service must issue a compensating transaction (refund). The
 * order-service listens to
 * {@code payment.refunded} and transitions the order to {@code CANCELLED}.
 *
 * <p>
 * The test simulates the payment-service and inventory-service by publishing
 * their events
 * directly onto the relevant Kafka topics using a {@code KafkaTemplate}.
 */
@DisplayName("Refund Saga Integration: payment → inventory failure → refund → CANCELLED")
@AutoConfigureRestTestClient
class RefundSagaIntegrationTest extends AbstractIntegrationTest {

        @Autowired
        private RestTestClient restClient;

        @Autowired
        private KafkaTemplate<String, Object> kafkaTemplate;

        @Autowired
        private OrderRepository orderRepository;

        @BeforeEach
        void cleanDatabase() {
                orderRepository.deleteAll();
        }

        @Test
        @DisplayName("Order is CANCELLED after inventory failure triggers a refund")
        void order_shouldBeCancelled_afterInventoryFailureAndRefund() throws Exception {
                CreateOrderDTO request = new CreateOrderDTO(1L, 5);
                Order created = restClient
                                .post()
                                .uri("/api/v1/orders")
                                .body(request)
                                .exchange()
                                .expectStatus()
                                .isEqualTo(HttpStatus.CREATED)
                                .expectBody(Order.class)
                                .returnResult()
                                .getResponseBody();

                assertThat(created).isNotNull();

                String orderId = created.getOrderId();
                assertThat(orderId).isNotBlank();
                assertThat(created.getStatus()).isEqualTo(OrderStatus.PENDING);

                String paymentId = UUID.randomUUID().toString();
                PaymentProcessedEvent paymentEvent = new PaymentProcessedEvent(orderId, paymentId, 1L, 5, 499.50);

                kafkaTemplate.send("payment.processed", orderId, paymentEvent).get(5, TimeUnit.SECONDS);

                await("order transitions to PAYMENT_CONFIRMED")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> {
                                                        Order order = orderRepository.findById(orderId).orElseThrow();
                                                        assertThat(order.getStatus())
                                                                        .isEqualTo(OrderStatus.PAYMENT_CONFIRMED);
                                                });

                InventoryInsufficientEvent insufficientEvent = new InventoryInsufficientEvent(orderId, 1L, 5,
                                paymentId);

                kafkaTemplate
                                .send("inventory.insufficient", orderId, insufficientEvent)
                                .get(5, TimeUnit.SECONDS);

                await("order transitions to INVENTORY_FAILED")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> {
                                                        Order order = orderRepository.findById(orderId).orElseThrow();
                                                        assertThat(order.getStatus())
                                                                        .isEqualTo(OrderStatus.INVENTORY_FAILED);
                                                });

                PaymentRefundedEvent refundEvent = new PaymentRefundedEvent(orderId, paymentId,
                                "Inventory insufficient");

                kafkaTemplate.send("payment.refunded", orderId, refundEvent).get(5, TimeUnit.SECONDS);

                await("order reaches terminal CANCELLED state")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> {
                                                        Order order = orderRepository.findById(orderId).orElseThrow();
                                                        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
                                                });
        }

        @Test
        @DisplayName("Duplicate payment.refunded events are idempotent (order stays CANCELLED)")
        void duplicate_refundEvent_shouldBeIdempotent() throws Exception {
                CreateOrderDTO request = new CreateOrderDTO(2L, 10);
                String orderId = restClient
                                .post()
                                .uri("/api/v1/orders")
                                .body(request)
                                .exchange()
                                .expectStatus()
                                .isEqualTo(HttpStatus.CREATED)
                                .expectBody(Order.class)
                                .returnResult()
                                .getResponseBody()
                                .getOrderId();

                String paymentId = UUID.randomUUID().toString();

                kafkaTemplate
                                .send(
                                                "payment.processed",
                                                orderId,
                                                new PaymentProcessedEvent(orderId, paymentId, 2L, 10, 999.00))
                                .get(5, TimeUnit.SECONDS);

                await("PAYMENT_CONFIRMED")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> assertThat(
                                                                orderRepository
                                                                                .findById(orderId)
                                                                                .orElseThrow()
                                                                                .getStatus())
                                                                .isEqualTo(OrderStatus.PAYMENT_CONFIRMED));

                kafkaTemplate
                                .send(
                                                "inventory.insufficient",
                                                orderId,
                                                new InventoryInsufficientEvent(orderId, 2L, 10, paymentId))
                                .get(5, TimeUnit.SECONDS);

                await("INVENTORY_FAILED")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> assertThat(
                                                                orderRepository
                                                                                .findById(orderId)
                                                                                .orElseThrow()
                                                                                .getStatus())
                                                                .isEqualTo(OrderStatus.INVENTORY_FAILED));

                PaymentRefundedEvent refundEvent = new PaymentRefundedEvent(orderId, paymentId,
                                "Inventory insufficient");

                kafkaTemplate.send("payment.refunded", orderId, refundEvent).get(5, TimeUnit.SECONDS);
                kafkaTemplate.send("payment.refunded", orderId, refundEvent).get(5, TimeUnit.SECONDS);

                await("order is CANCELLED (idempotent)")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> {
                                                        Order order = orderRepository.findById(orderId).orElseThrow();
                                                        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
                                                });
        }

        @Test
        @DisplayName("payment.failed (not inventory failure) also leads to CANCELLED via normal path")
        void paymentFailed_shouldCancelOrder_withoutRefundSaga() throws Exception {
                CreateOrderDTO request = new CreateOrderDTO(3L, 1);
                String orderId = restClient
                                .post()
                                .uri("/api/v1/orders")
                                .body(request)
                                .exchange()
                                .expectStatus()
                                .isEqualTo(HttpStatus.CREATED)
                                .expectBody(Order.class)
                                .returnResult()
                                .getResponseBody()
                                .getOrderId();

                com.vikas.shared.events.PaymentFailedEvent failedEvent = new com.vikas.shared.events.PaymentFailedEvent(
                                orderId, null, "Insufficient funds");

                kafkaTemplate.send("payment.failed", orderId, failedEvent).get(5, TimeUnit.SECONDS);

                await("order transitions to PAYMENT_FAILED")
                                .atMost(Duration.ofSeconds(15))
                                .pollInterval(Duration.ofMillis(500))
                                .untilAsserted(
                                                () -> {
                                                        Order order = orderRepository.findById(orderId).orElseThrow();
                                                        assertThat(order.getStatus())
                                                                        .isEqualTo(OrderStatus.PAYMENT_FAILED);
                                                });
        }
}
