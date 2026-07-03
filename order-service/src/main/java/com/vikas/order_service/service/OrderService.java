package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.shared.events.OrderCreatedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    public Optional<Order> getOrderById(String orderId) {
        return orderRepository.findById(orderId);
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    public Order createOrder(Long productId, Integer quantity) {
        String orderId = UUID.randomUUID().toString();

        Order order = new Order();
        order.setOrderId(orderId);
        order.setProductId(productId);
        order.setQuantity(quantity);
        order.setStatus(OrderStatus.PENDING);
        orderRepository.save(order);

        OrderCreatedEvent event = new OrderCreatedEvent(orderId, productId, quantity, order.getCreatedAt());
        kafkaTemplate.send("order.created", orderId, event);
        log.info(
                "Order created and event published: orderId={}, productId={}, quantity={}",
                orderId,
                productId,
                quantity);

        return order;
    }
}
