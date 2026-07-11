package com.vikas.inventory_service.model;

import com.vikas.shared.events.PaymentProcessedEvent;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
public class DLTEvent {
    @Id private String orderId;
    private String paymentId;
    private Long productId;
    private Integer quantity;
    private Double amount;
    private DLTStatus status;

    public void processedEventToDlt(PaymentProcessedEvent event) {
        this.orderId = event.getOrderId();
        this.paymentId = event.getPaymentId();
        this.productId = event.getProductId();
        this.quantity = event.getQuantity();
        this.amount = event.getAmount();
    }

    public PaymentProcessedEvent processToPaymentProcessedEvent() {
        PaymentProcessedEvent event = new PaymentProcessedEvent();
        event.setPaymentId(this.paymentId);
        event.setOrderId(this.orderId);
        event.setProductId(this.productId);
        event.setQuantity(this.quantity);
        event.setAmount(this.amount);
        return event;
    }
}
