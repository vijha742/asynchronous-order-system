package com.vikas.order_service.repository;

import com.vikas.order_service.model.OrderPollerDTO;
import com.vikas.order_service.model.PollerStatus;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

public interface Outbox extends JpaRepository<OrderPollerDTO, String> {

    Page<OrderPollerDTO> findByStatus(PollerStatus status, Pageable page);
}
