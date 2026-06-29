package com.vikas.inventory_service.model;

import jakarta.persistence.Entity;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
@Entity
public class Item {

    private Long productId;
    private Integer quantity;
    private Long price;
}
