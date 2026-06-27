package com.vikas.shared.events;

public class PaymentFailedEvent extends PaymentEvent {

    private Long orderId;
    private Long paymentId;

    public PaymentFailedEvent(Long orderId, Long paymentId) {
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
        return "PaymentFailedEvent [orderId=" + orderId + ", paymentId=" + paymentId + "]";
    }
}
