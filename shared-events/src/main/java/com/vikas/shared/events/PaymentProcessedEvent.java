package com.vikas.shared.events;

public class PaymentProcessedEvent extends PaymentEvent {

    private Long orderId;
    private Long paymentId;

    public PaymentProcessedEvent(Long orderId, Long paymentId) {
        this.orderId = orderId;
        this.paymentId = paymentId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public void setPaymentId(Long paymentId) {
        this.paymentId = paymentId;
    }

    @Override
    public String toString() {
        return "PaymentProcessedEvent [orderId=" + orderId + ", paymentId=" + paymentId + "]";
    }
}
