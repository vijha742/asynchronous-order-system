package com.vikas.shared.events;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentRefundedEvent {
    private String orderId;
    private String paymentId;
    private String reason;
}
