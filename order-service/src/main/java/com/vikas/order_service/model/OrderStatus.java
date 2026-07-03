package com.vikas.order_service.model;

public enum OrderStatus {
    PENDING,
    PAYMENT_PROCESSING,
    PAYMENT_CONFIRMED,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    CONFIRMED,
    CANCELLED;

    public boolean comesAfter(OrderStatus other) {
        return this.compareTo(other) > 0;
    }

    public boolean comesBefore(OrderStatus other) {
        return this.compareTo(other) < 0;
    }
}
