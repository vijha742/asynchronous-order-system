package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.shared.events.OrderCreatedEvent;

import lombok.RequiredArgsConstructor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@RequiredArgsConstructor
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<Long, OrderCreatedEvent> kafkaTemplate;

    public Optional<Order> getOrderById(long id) {
        return orderRepository.findById(id);
    }

    public Order publishOrderCreatedEvent(Long productId, Integer quantity) {
        Order order = new Order();
        Random random = new Random();
        Long orderId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        order.setOrderId(orderId);
        order.setProductId(productId);
        order.setStatus(OrderStatus.PENDING);
        order.setQuantity(quantity);
        Long time = System.currentTimeMillis();
        order.setCreatedAt(time);
        order.setUpdatedAt(time);
        orderRepository.save(order);
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, productId, quantity, time, time);
        kafkaTemplate.send("order.created", event);
        return order;
    }
}
