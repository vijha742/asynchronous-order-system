package com.vikas.payment_service.repository;

import com.vikas.payment_service.model.ProcessedEvents;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventsRepository extends JpaRepository<ProcessedEvents, String> {
}
