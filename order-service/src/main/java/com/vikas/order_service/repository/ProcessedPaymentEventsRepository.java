package com.vikas.order_service.repository;

import com.vikas.order_service.model.ProcessedPaymentEvents;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedPaymentEventsRepository
        extends JpaRepository<ProcessedPaymentEvents, String> {
}
