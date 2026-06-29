package com.vikas.shared.events;

public class InventoryReservedEvent extends InventoryEvent {

    private Long orderId;

    public InventoryReservedEvent(Long orderId) {
        this.orderId = orderId;
    }

    public Long getOrderId() {
        return orderId;
    }

    @Override
    public String toString() {
        return "InventoryReservedEvent [orderId=" + orderId + "]";
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }
}
