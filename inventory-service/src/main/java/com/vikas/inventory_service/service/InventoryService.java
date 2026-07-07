package com.vikas.inventory_service.service;

import com.vikas.inventory_service.model.EventStatus;
import com.vikas.inventory_service.model.Item;
import com.vikas.inventory_service.model.ProcessedEvents;
import com.vikas.inventory_service.model.ReservationStatus;
import com.vikas.inventory_service.model.StockReservation;
import com.vikas.inventory_service.repository.InventoryRepository;
import com.vikas.inventory_service.repository.ProcessedEventsRepository;
import com.vikas.inventory_service.repository.StockReservationRepository;
import com.vikas.shared.events.InventoryEvent;
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
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class InventoryService {

    private final KafkaTemplate<String, InventoryEvent> kafkaTemplate;
    private final KafkaTemplate<String, PaymentProcessedEvent> paymentKafkaTemplate;
    private final InventoryRepository inventoryRepository;
    private final StockReservationRepository reservationRepository;
    private final ProcessedEventsRepository processedEventsRepository;

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

        if (reservationRepository.existsByOrderId(event.getOrderId())) {
            log.warn("Duplicate event received for orderId={} — skipping", event.getOrderId());
            return;
        }

        try {
            boolean reserved = reserveStock(event);
            if (reserved) {
                InventoryReservedEvent reservedEvent =
                        new InventoryReservedEvent(
                                event.getOrderId(), event.getProductId(), event.getQuantity());
                kafkaTemplate.send(inventoryReservedTopic, event.getOrderId(), reservedEvent);
                processedEventsRepository.save(
                        new ProcessedEvents(event.getOrderId(), EventStatus.RESERVED));
                log.info(
                        "Stock reserved: orderId={}, productId={}, qty={}",
                        event.getOrderId(),
                        event.getProductId(),
                        event.getQuantity());
            } else {
                InventoryInsufficientEvent insufficientEvent =
                        new InventoryInsufficientEvent(
                                event.getOrderId(),
                                event.getProductId(),
                                event.getQuantity(),
                                event.getPaymentId());
                processedEventsRepository.save(
                        new ProcessedEvents(event.getOrderId(), EventStatus.FAILED));
                kafkaTemplate.send(
                        inventoryInsufficientTopic, event.getOrderId(), insufficientEvent);
                log.warn(
                        "Insufficient stock: orderId={}, productId={}, requested={}",
                        event.getOrderId(),
                        event.getProductId(),
                        event.getQuantity());
            }
        } catch (ObjectOptimisticLockingFailureException ex) {
            log.error(
                    "Optimistic lock conflict reserving stock for orderId={} — will retry via"
                            + " Kafka",
                    event.getOrderId());
            throw ex;
        }
    }

    /**
     * Atomically checks and decrements stock within a single transaction. The {@code @Version}
     * field on {@link Item} ensures concurrent reservations are serialised via optimistic locking —
     * preventing oversell.
     *
     * @return true if reservation succeeded, false if stock is insufficient or product not found
     */
    @Transactional
    public boolean reserveStock(PaymentProcessedEvent event) {
        Optional<ProcessedEvents> processedKey =
                processedEventsRepository.findById(event.getOrderId());
        if (processedKey.isPresent()) {
            log.warn(
                    "Event has already been processed for order {} with orderId {}",
                    event,
                    event.getOrderId());
            return true;
        }
        Optional<Item> opt = inventoryRepository.findByProductId(event.getProductId());

        if (opt.isEmpty()) {
            log.warn("Product not found in inventory: productId={}", event.getProductId());
            return false;
        }

        Item item = opt.get();
        if (item.getQuantity() < event.getQuantity()) {
            log.warn(
                    "Insufficient quantity for productId={}: available={}, requested={}",
                    event.getProductId(),
                    item.getQuantity(),
                    event.getQuantity());
            return false;
        }

        item.setQuantity(item.getQuantity() - event.getQuantity());
        inventoryRepository.save(item);

        StockReservation reservation = new StockReservation();
        reservation.setOrderId(event.getOrderId());
        reservation.setPaymentId(event.getPaymentId());
        reservation.setProductId(event.getProductId());
        reservation.setQuantity(event.getQuantity());
        reservation.setStatus(ReservationStatus.RESERVED);
        reservation.setCreatedAt(System.currentTimeMillis());
        reservationRepository.save(reservation);

        return true;
    }

    @DltHandler
    public void listenDLT(PaymentProcessedEvent event) {
        paymentKafkaTemplate.send("inventory.dlt", event);
        log.info("Event added to DLT for paymentEvent {}", event);
    }
}
