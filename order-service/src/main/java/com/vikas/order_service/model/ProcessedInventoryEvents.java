package com.vikas.order_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "processed_inventory_event_ids")
public class ProcessedInventoryEvents {

        @Id
        private String orderId;
}
