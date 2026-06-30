package com.vikas.order_service.model;

public enum OrderStatus {
    PENDING,
    PAYMENT_PROCESSING,
    PAYMENT_CONFIRMED,   // renamed from PAYMENT_COMPLETED to align with project spec
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    CONFIRMED,
    CANCELLED
}
