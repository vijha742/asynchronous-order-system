package com.vikas.inventory_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Data
@Entity
public class Item {

    @Id
    private Long productId;
    private Integer quantity;
    private Long price;
}
