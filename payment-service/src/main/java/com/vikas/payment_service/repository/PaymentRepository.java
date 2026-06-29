package com.vikas.payment_service.repository;

import com.vikas.payment_service.model.Payment;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<Payment, Long> {}
