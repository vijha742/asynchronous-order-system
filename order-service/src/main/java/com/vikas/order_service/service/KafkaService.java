package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.InventoryReservedEvent;
import com.vikas.shared.events.PaymentFailedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaService {

    private final OrderRepository orderRepository;

    @KafkaListener(topics = "payment.processed", groupId = "order-service")
    public void onPaymentProcessed(PaymentProcessedEvent event) {
        log.info("Payment confirmed for orderId={}", event.getOrderId());
        updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_CONFIRMED);
    }

    @KafkaListener(topics = "payment.failed", groupId = "order-service")
    public void onPaymentFailed(PaymentFailedEvent event) {
        log.info("Payment failed for orderId={}, reason={}", event.getOrderId(), event.getReason());
        updateOrderStatus(event.getOrderId(), OrderStatus.PAYMENT_FAILED);
    }

    @KafkaListener(topics = "inventory.reserved", groupId = "order-service")
    public void onInventoryReserved(InventoryReservedEvent event) {
        log.info("Inventory reserved for orderId={}, productId={}, qty={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
        updateOrderStatus(event.getOrderId(), OrderStatus.CONFIRMED);
    }

    @KafkaListener(topics = "inventory.insufficient", groupId = "order-service")
    public void onInventoryInsufficient(InventoryInsufficientEvent event) {
        log.info("Inventory insufficient for orderId={}, productId={}, requested={}",
                event.getOrderId(), event.getProductId(), event.getQuantityRequested());
        updateOrderStatus(event.getOrderId(), OrderStatus.INVENTORY_FAILED);
    }

    private void updateOrderStatus(String orderId, OrderStatus newStatus) {
        Optional<Order> opt = orderRepository.findById(orderId);
        if (opt.isPresent()) {
            Order order = opt.get();
            log.debug("Order {} transitioning {} → {}", orderId, order.getStatus(), newStatus);
            order.setStatus(newStatus);
            orderRepository.save(order);
        } else {
            log.warn("Received event for unknown orderId={}", orderId);
        }
    }
}
