package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.repository.OrderRepository;

import lombok.RequiredArgsConstructor;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@RequiredArgsConstructor
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<Long, Order> kafkaTemplate;

    public Optional<Order> getOrderById(long id) {
        return orderRepository.findById(id);
    }

    public Order publishOrderCreatedEvent(Long productId, Integer quantity) {
        Order order = new Order();
        order.setProductId(productId);
        order.setStatus(OrderStatus.PENDING);
        order.setQuantity(quantity);
        order.setCreatedAt(System.currentTimeMillis());
        order.setUpdatedAt(System.currentTimeMillis());
        kafkaTemplate.send("order-created", order);
        orderRepository.save(order);
        return order;
    }
}
