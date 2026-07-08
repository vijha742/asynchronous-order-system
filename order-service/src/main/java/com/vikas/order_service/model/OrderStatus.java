package com.vikas.order_service.model;

import java.util.Map;
import java.util.Set;

public enum OrderStatus {
    PENDING,
    PAYMENT_PROCESSING,
    PAYMENT_CONFIRMED,
    PAYMENT_FAILED,
    INVENTORY_RESERVED,
    INVENTORY_FAILED,
    CONFIRMED,
    CANCELLED;

    private static final Map<OrderStatus, Set<OrderStatus>> VALID_TRANSITIONS = Map.of(
        PENDING, Set.of(PAYMENT_PROCESSING),
        PAYMENT_PROCESSING, Set.of(PAYMENT_CONFIRMED, PAYMENT_FAILED),
        PAYMENT_CONFIRMED, Set.of(INVENTORY_RESERVED, INVENTORY_FAILED),
        PAYMENT_FAILED, Set.of(CANCELLED),
        INVENTORY_FAILED, Set.of(CANCELLED),
        INVENTORY_RESERVED, Set.of(CONFIRMED),
        CONFIRMED, Set.of(), // Terminal
        CANCELLED, Set.of()  // Terminal
    );

    public boolean canTransitionTo(OrderStatus next) {
        return VALID_TRANSITIONS.getOrDefault(this, Set.of()).contains(next);
    }
}
