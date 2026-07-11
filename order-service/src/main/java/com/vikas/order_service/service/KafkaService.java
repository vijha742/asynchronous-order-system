package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.model.ProcessedInventoryEvents;
import com.vikas.order_service.model.ProcessedPaymentEvents;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.order_service.repository.ProcessedInventoryEventsRepository;
import com.vikas.order_service.repository.ProcessedPaymentEventsRepository;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.InventoryReservedEvent;
import com.vikas.shared.events.PaymentFailedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;
import com.vikas.shared.events.PaymentRefundedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaService {

    private final OrderRepository orderRepository;
    private final ProcessedInventoryEventsRepository processedInventoryEventsRepository;
    private final ProcessedPaymentEventsRepository processedPaymentEventsRepository;

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
        if (processedPaymentEventsRepository.findById(event.getOrderId()).isPresent())
            return;
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
        if (processedInventoryEventsRepository.findById(event.getOrderId()).isPresent())
            return;
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
