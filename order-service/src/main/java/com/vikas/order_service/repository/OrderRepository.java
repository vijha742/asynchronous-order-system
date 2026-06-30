package com.vikas.order_service.repository;

import com.vikas.order_service.model.Order;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OrderRepository extends JpaRepository<Order, String> {
    // @Id is String (UUID), so JpaRepository<Order, String> gives us findById(String)
    // No custom findByOrderId needed — the UUID is the primary key directly
}
