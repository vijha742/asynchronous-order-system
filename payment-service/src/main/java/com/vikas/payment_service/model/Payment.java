package com.vikas.payment_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Entity
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Payment {
	@Id
	@GeneratedValue(strategy = GenerationType.AUTO)
	private Long id;

	private Long paymentId;
	private Long orderId;
	private Double amount;
	private Long createdAt;
	private Long updatedAt;
	private PaymentStatus status;

}
