package com.vikas.payment_service.repository;

import com.vikas.payment_service.model.Payment;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PaymentRepository extends JpaRepository<Payment, String> {
    // @Id is String (UUID paymentId)
}
