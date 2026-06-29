package com.vikas.inventory_service.service;

import com.vikas.inventory_service.model.Item;
import com.vikas.inventory_service.repository.InventoryRepository;
import com.vikas.shared.events.InventoryEvent;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.InventoryReservedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
@Service
public class InventoryService {

    private final KafkaTemplate<Long, InventoryEvent> kafkaTemplate;
    private final InventoryRepository inventoryRepository;

    @Value("${inventory.reserved}")
    private String inventoryReservedTopic;

    @Value("${inventory.insufficient}")
    private String inventoryInsufficientTopic;

    @KafkaListener(topics = "payment.processed", groupId = "inventory-service")
    public void listen(PaymentProcessedEvent event) {
        log.info("Payment event {}", event);
        if (checkInventoryForAvailability(event)) publishReservedEvent(event.getOrderId());
        else publishInsufficientEvent(event.getOrderId());
    }

    public boolean checkInventoryForAvailability(PaymentProcessedEvent event) {
        Optional<Item> temp = inventoryRepository.findByProductId(event.getOrderId());
        if (temp.isPresent()) {
            Item item = temp.get();
            if (item.getQuantity() < event.getQuantity()) {
                return false;
            }
            return true;
        }
        return false;
    }

    public void publishReservedEvent(Long orderId) {
        InventoryEvent reserved = new InventoryReservedEvent(orderId);
        kafkaTemplate.send(inventoryReservedTopic, reserved);
    }

    public void publishInsufficientEvent(Long orderId) {
        InventoryEvent insufficient = new InventoryInsufficientEvent(orderId);
        kafkaTemplate.send(inventoryInsufficientTopic, insufficient);
    }
}
