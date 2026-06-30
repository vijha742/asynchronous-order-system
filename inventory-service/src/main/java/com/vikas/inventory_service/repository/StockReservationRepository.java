package com.vikas.inventory_service.repository;

import com.vikas.inventory_service.model.StockReservation;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface StockReservationRepository extends JpaRepository<StockReservation, Long> {

    Optional<StockReservation> findByOrderId(String orderId);

    boolean existsByOrderId(String orderId);
}
