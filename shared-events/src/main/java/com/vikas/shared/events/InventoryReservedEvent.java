package com.vikas.shared.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InventoryReservedEvent {
    private String orderId;
    private Long productId;
    private Integer quantity;
}
