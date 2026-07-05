package com.vikas.order_service.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class CreateOrderDTO {

    @NotNull(message = "productId cannot be empty...")
    @Min(1)
    private Long productId;

    @NotNull(message = "quantity cannot be empty...")
    @Min(1)
    private Integer quantity;
}
