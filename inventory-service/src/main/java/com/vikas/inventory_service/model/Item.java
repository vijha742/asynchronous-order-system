package com.vikas.inventory_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Entity
@Table(name = "items")
public class Item {

    @Id
    private Long productId;

    private String name;
    private Integer quantity;
    private Double price;

    @Version
    private Long version;
}
