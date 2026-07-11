package com.vikas.order_service.repository;

import com.vikas.order_service.model.ProcessedInventoryEvents;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedInventoryEventsRepository
        extends JpaRepository<ProcessedInventoryEvents, String> {
}
