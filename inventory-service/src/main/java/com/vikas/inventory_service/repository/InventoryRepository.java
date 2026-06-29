package com.vikas.inventory_service.repository;

import com.vikas.inventory_service.model.Item;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface InventoryRepository extends JpaRepository<Item, Long> {

    Optional<Item> findByProductId(Long productId);
}
