package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderPollerDTO;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.model.PollerStatus;
import com.vikas.order_service.model.ProcessedInventoryEvents;
import com.vikas.order_service.model.ProcessedPaymentEvents;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.order_service.repository.Outbox;
import com.vikas.order_service.repository.ProcessedInventoryEventsRepository;
import com.vikas.order_service.repository.ProcessedPaymentEventsRepository;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.InventoryReservedEvent;
import com.vikas.shared.events.OrderCreatedEvent;
import com.vikas.shared.events.PaymentFailedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;
import com.vikas.shared.events.PaymentRefundedEvent;

import jakarta.transaction.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaService {

    private final OrderRepository orderRepository;
    private final ProcessedInventoryEventsRepository processedInventoryEventsRepository;
    private final ProcessedPaymentEventsRepository processedPaymentEventsRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final Outbox outbox;

    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void processOutbox() {
        Page<OrderPollerDTO> pollerObjects =
                outbox.findByStatus(PollerStatus.PENDING, PageRequest.of(0, 50));
        for (OrderPollerDTO order : pollerObjects) {
            OrderCreatedEvent event =
                    new OrderCreatedEvent(
                            order.getOrderId(), order.getProductId(), order.getQuantity());
            try {
                kafkaTemplate.send("order.created", order.getOrderId(), event).get();
                order.setStatus(PollerStatus.PROCESSED);
                outbox.save(order);
                log.info(
                        "Order created and event published: orderId={}, productId={}, quantity={}",
                        order.getOrderId(),
                        order.getProductId(),
                        order.getQuantity());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                log.debug(
                        "Order couldn't get published for order with orderId : {}",
                        order.getOrderId());
            }
        }
    }

    @KafkaListener(topics = "payment.processed", groupId = "order-service")
    public void onPaymentProcessed(PaymentProcessedEvent event) {
        log.info("Payment confirmed for orderId={}", event.getOrderId());
        try {
            processedPaymentEventsRepository.saveAndFlush(
                    new ProcessedPaymentEvents(event.getOrderId()));
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate event — skipping: orderId={}", event.getOrderId());
            return;
        }
        updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_CONFIRMED);
    }

    @KafkaListener(topics = "payment.failed", groupId = "order-service")
    public void onPaymentFailed(PaymentFailedEvent event) {
        if (processedPaymentEventsRepository.findById(event.getOrderId()).isPresent()) return;
        log.info("Payment failed for orderId={}, reason={}", event.getOrderId(), event.getReason());
        updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_FAILED);
    }

    @KafkaListener(topics = "inventory.reserved", groupId = "order-service")
    public void onInventoryReserved(InventoryReservedEvent event) {
        log.info(
                "Inventory reserved for orderId={}, productId={}, qty={}",
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity());
        try {
            processedInventoryEventsRepository.saveAndFlush(
                    new ProcessedInventoryEvents(event.getOrderId()));
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate event — skipping: orderId={}", event.getOrderId());
            return;
        }
        updateOrderStatus(event.getOrderId(), OrderStatus.CONFIRMED);
    }

    @KafkaListener(topics = "inventory.insufficient", groupId = "order-service")
    public void onInventoryInsufficient(InventoryInsufficientEvent event) {
        if (processedInventoryEventsRepository.findById(event.getOrderId()).isPresent()) return;
        log.info(
                "Inventory insufficient for orderId={}, productId={}, requested={}",
                event.getOrderId(),
                event.getProductId(),
                event.getQuantityRequested());
        updateOrderStatus(event.getOrderId(), OrderStatus.INVENTORY_FAILED);
    }

    @KafkaListener(topics = "payment.refunded", groupId = "order-service")
    public void onPaymentRefunded(PaymentRefundedEvent event) {
        String idempotencyKey = "REFUND:" + event.getOrderId();
        try {
            processedPaymentEventsRepository.saveAndFlush(
                    new ProcessedPaymentEvents(idempotencyKey));
        } catch (DataIntegrityViolationException e) {
            log.warn("Duplicate refund event — skipping: orderId={}", event.getOrderId());
            return;
        }
        log.info(
                "Refund confirmed for orderId={}, paymentId={} — transitioning to CANCELLED",
                event.getOrderId(),
                event.getPaymentId());
        updateOrderStatus(event.getOrderId(), OrderStatus.CANCELLED);
    }

    private void updateOrderStatus(String orderId, OrderStatus newStatus) {
        Optional<Order> opt = orderRepository.findById(orderId);
        if (opt.isPresent()) {
            Order order = opt.get();
            if (order.getStatus().canTransitionTo(newStatus)) {
                log.debug("Order {} transitioning {} → {}", orderId, order.getStatus(), newStatus);
                order.setStatus(newStatus);
                orderRepository.saveAndFlush(order);
            } else {
                log.debug(
                        "Order state stored has priority or transition is invalid. Hence the order"
                            + " state will not change, Order : {}, Status: {}, Requested Status:"
                            + " {}",
                        orderId,
                        order.getStatus(),
                        newStatus);
            }
        } else {
            log.warn("Received event for unknown orderId={}", orderId);
        }
    }
}
