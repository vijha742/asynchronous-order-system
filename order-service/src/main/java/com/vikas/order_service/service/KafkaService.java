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
    public void listenPayment(PaymentProcessedEvent event) {
        log.info("received payment event : {}", event);
        updateOrderStatus(OrderStatus.PAYMENT_COMPLETED, event.getOrderId());
    }

    @KafkaListener(topics = "inventory.reserved", groupId = "order-service")
    public void listenPayment(InventoryReservedEvent event) {
        log.info("received inventory event : {}", event);
        updateOrderStatus(OrderStatus.INVENTORY_RESERVED, event.getOrderId());
    }

    @KafkaListener(topics = "payment.failed", groupId = "order-service")
    public void listenPayment(PaymentFailedEvent event) {
        log.info("received payment failed event : {}", event);
        updateOrderStatus(OrderStatus.PAYMENT_FAILED, event.getOrderId());
    }

    @KafkaListener(topics = "inventory.insufficient", groupId = "order-service")
    public void listenPayment(InventoryInsufficientEvent event) {
        log.info("received inventory insufficient event : {}", event);
        updateOrderStatus(OrderStatus.INVENTORY_FAILED, event.getOrderId());
    }

    public void updateOrderStatus(OrderStatus status, Long orderId) {
        Optional<Order> temp = orderRepository.findByOrderId(orderId);
        if (temp.isPresent()) {
            Order order = temp.get();
            order.setStatus(status);
            orderRepository.save(order);
        }
    }
}
