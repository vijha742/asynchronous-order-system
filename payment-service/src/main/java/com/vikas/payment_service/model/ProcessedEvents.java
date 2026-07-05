package com.vikas.payment_service.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "processed_event_ids")
public class ProcessedEvents {

    @Id
    private String orderId;

    private EventStatus status;
}
