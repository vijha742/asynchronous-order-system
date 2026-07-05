package com.vikas.order_service.controller;

import com.vikas.order_service.model.CreateOrderDTO;
import com.vikas.order_service.model.Order;
import com.vikas.order_service.service.OrderService;

import jakarta.validation.Valid;

import lombok.RequiredArgsConstructor;

import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @GetMapping("/{orderId}")
    public ResponseEntity<Order> getOrder(@PathVariable String orderId) {
        return orderService
                .getOrderById(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<Order>> getAllOrders() {
        List<Order> orders = orderService.getAllOrders();
        return orders.isEmpty() ? ResponseEntity.noContent().build() : ResponseEntity.ok(orders);
    }

    @PostMapping
    public ResponseEntity<String> createOrder(
            @RequestBody @Valid CreateOrderDTO orderReq, BindingResult bindingResult) {
        if (bindingResult.hasFieldErrors())
            return ResponseEntity.badRequest()
                    .body("The order isn't valid..." + bindingResult.getAllErrors());
        Order order = orderService.createOrder(orderReq.getProductId(), orderReq.getQuantity());
        return ResponseEntity.status(201).body(order.toString());
    }
}
