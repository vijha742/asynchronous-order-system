package com.vikas.inventory_service.service;

import com.vikas.inventory_service.service.InventoryReservationService.ReservationResultType;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.InventoryReservedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Slf4j
@RequiredArgsConstructor
@Service
public class InventoryService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final InventoryReservationService inventoryReservationService;

    @Value("${inventory.reserved}")
    private String inventoryReservedTopic;

    @Value("${inventory.insufficient}")
    private String inventoryInsufficientTopic;

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 5000))
    @KafkaListener(topics = "payment.processed", groupId = "inventory-service")
    public void onPaymentProcessed(PaymentProcessedEvent event) {
        log.info(
                "Processing inventory reservation: orderId={}, productId={}, qty={}",
                event.getOrderId(),
                event.getProductId(),
                event.getQuantity());

        ReservationResultType result = inventoryReservationService.reserveStock(event);

        if (result == ReservationResultType.SUCCESS) {
            InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                    event.getOrderId(), event.getProductId(), event.getQuantity());
            kafkaTemplate.send(inventoryReservedTopic, event.getOrderId(), reservedEvent);
            log.info("Stock reserved: orderId={}, productId={}, qty={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
        } else if (result == ReservationResultType.INSUFFICIENT_STOCK || result == ReservationResultType.PRODUCT_NOT_FOUND) {
            InventoryInsufficientEvent insufficientEvent = new InventoryInsufficientEvent(
                    event.getOrderId(),
                    event.getProductId(),
                    event.getQuantity(),
                    event.getPaymentId());
            kafkaTemplate.send(inventoryInsufficientTopic, event.getOrderId(), insufficientEvent);
            log.warn("Insufficient stock: orderId={}, productId={}, requested={}",
                    event.getOrderId(), event.getProductId(), event.getQuantity());
        }
    }

    @DltHandler
    public void listenDLT(PaymentProcessedEvent event) {
        kafkaTemplate.send("inventory.dlt", event);
        log.info("Event added to DLT for paymentEvent {}", event);
    }
}
