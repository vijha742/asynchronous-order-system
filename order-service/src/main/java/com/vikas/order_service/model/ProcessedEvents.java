package com.vikas.order_service.model;

import lombok.AllArgsConstructor;

// @Entity
@AllArgsConstructor
// @Table(name = "processed_event_ids")
public class ProcessedEvents {

    // @Id
    private String orderId;

    private EventStatus status;
}
