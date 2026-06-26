package com.vikas.order_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
@Entity
@Table(name = "orders")
public class Order {

	@GeneratedValue(strategy = GenerationType.IDENTITY)
	@Id
	private Long id;

	private long productId;
	private OrderStatus status;
	private int quantity;
	private Long createdAt;
	private Long updatedAt;
}
