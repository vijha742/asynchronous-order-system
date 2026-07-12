package com.vikas.order_service.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record CreateOrderDTO(
                @NotNull(message = "productId cannot be empty...") @Min(value = 1, message = "productId should be more than 0") @Max(value = 3, message = "productId should be less than 4") Long productId,
                @NotNull(message = "quantity cannot be empty...") @Min(value = 1, message = "ordered quantity must be at least 1...") Integer quantity) {
}
