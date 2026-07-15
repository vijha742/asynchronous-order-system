package com.vikas.order_service.repository;

import com.vikas.order_service.model.OrderPollerEvent;
import com.vikas.order_service.model.PollerStatus;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface Outbox extends JpaRepository<OrderPollerEvent, String> {

    Page<OrderPollerEvent> findByStatus(PollerStatus status, Pageable page);
}
