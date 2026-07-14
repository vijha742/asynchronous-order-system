package com.vikas.order_service.service;

import com.vikas.order_service.model.Order;
import com.vikas.order_service.model.OrderPollerEvent;
import com.vikas.order_service.model.OrderStatus;
import com.vikas.order_service.model.PollerStatus;
import com.vikas.order_service.repository.OrderRepository;
import com.vikas.order_service.repository.Outbox;

import jakarta.transaction.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final Outbox outbox;

    public Optional<Order> getOrderById(String orderId) {
        return orderRepository.findById(orderId);
    }

    public List<Order> getAllOrders() {
        return orderRepository.findAll();
    }

    @Transactional
    public Order createOrder(Long productId, Integer quantity) {
        String orderId = UUID.randomUUID().toString();

        Order order = new Order();
        order.setOrderId(orderId);
        order.setProductId(productId);
        order.setQuantity(quantity);
        order.setStatus(OrderStatus.PENDING);
        orderRepository.save(order);

        OrderPollerEvent pollerDTO = new OrderPollerEvent();
        pollerDTO.setOrderId(orderId);
        pollerDTO.setProductId(productId);
        pollerDTO.setQuantity(quantity);
        pollerDTO.setStatus(PollerStatus.PENDING);
        outbox.save(pollerDTO);

        return order;
    }
}
