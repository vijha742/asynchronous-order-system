package com.vikas.shared.events;

public class InventoryInsufficientEvent extends InventoryEvent {

    private Long orderId;

    public InventoryInsufficientEvent(Long orderId) {
        this.orderId = orderId;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    @Override
    public String toString() {
        return "InventoryInsufficientEvent [orderId=" + orderId + "]";
    }
}
