package com.vikas.shared.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentProcessedEvent extends PaymentEvent {
    private String orderId;
    private String paymentId;
    private Long productId;
    private Integer quantity;
    private Double amount;
}
