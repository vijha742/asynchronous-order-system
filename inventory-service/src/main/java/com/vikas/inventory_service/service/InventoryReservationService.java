package com.vikas.inventory_service.service;

import com.vikas.inventory_service.model.Item;
import com.vikas.inventory_service.model.ProcessedEvents;
import com.vikas.inventory_service.model.ReservationStatus;
import com.vikas.inventory_service.model.StockReservation;
import com.vikas.inventory_service.repository.InventoryRepository;
import com.vikas.inventory_service.repository.ProcessedEventsRepository;
import com.vikas.inventory_service.repository.StockReservationRepository;
import com.vikas.shared.events.PaymentProcessedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryReservationService {

    private final InventoryRepository inventoryRepository;
    private final StockReservationRepository reservationRepository;
    private final ProcessedEventsRepository processedEventsRepository;

    public enum ReservationResultType {
        SUCCESS,
        ALREADY_PROCESSED,
        INSUFFICIENT_STOCK,
        PRODUCT_NOT_FOUND
    }

    @Transactional
    public ReservationResultType reserveStock(PaymentProcessedEvent event) {
        Optional<ProcessedEvents> processedKey = processedEventsRepository.findById(event.getOrderId());
        if (processedKey.isPresent()) {
            log.warn("Event has already been processed for orderId {}", event.getOrderId());
            return ReservationResultType.ALREADY_PROCESSED;
        }
        
        try {
            processedEventsRepository.saveAndFlush(new ProcessedEvents(event.getOrderId()));
        } catch (org.springframework.dao.DataIntegrityViolationException ex) {
            log.warn("Duplicate inventory reservation event detected via DB constraint: orderId={}", event.getOrderId());
            return ReservationResultType.ALREADY_PROCESSED;
        }

        Optional<Item> opt = inventoryRepository.findByProductId(event.getProductId());
        if (opt.isEmpty()) {
            log.warn("Product not found in inventory: productId={}", event.getProductId());
            return ReservationResultType.PRODUCT_NOT_FOUND;
        }

        Item item = opt.get();
        if (item.getQuantity() < event.getQuantity()) {
            log.warn("Insufficient quantity for productId={}: available={}, requested={}",
                    event.getProductId(), item.getQuantity(), event.getQuantity());
            return ReservationResultType.INSUFFICIENT_STOCK;
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

        return ReservationResultType.SUCCESS;
    }
}
