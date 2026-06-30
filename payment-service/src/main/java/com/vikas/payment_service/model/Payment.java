package com.vikas.payment_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@Entity
@Table(name = "payments")
public class Payment {

    /** UUID string — set by KafkaService before persist */
    @Id
    private String paymentId;

    private String orderId;    // matches Order.orderId
    private Long productId;    // carried forward for refund saga (Week 4)
    private Integer quantity;
    private Double amount;

    @Enumerated(EnumType.STRING)
    private PaymentStatus status;

    private Long createdAt;
    private Long updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = System.currentTimeMillis();
        updatedAt = createdAt;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = System.currentTimeMillis();
    }
}
