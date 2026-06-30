package com.vikas.shared.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InventoryInsufficientEvent extends InventoryEvent {
    private String orderId;
    private Long productId;
    private Integer quantityRequested;
    private String paymentId; // needed for refund...
}
