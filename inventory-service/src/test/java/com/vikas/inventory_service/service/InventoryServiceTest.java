package com.vikas.inventory_service.service;

import com.vikas.inventory_service.model.EventStatus;
import com.vikas.inventory_service.model.Item;
import com.vikas.inventory_service.model.ProcessedEvents;
import com.vikas.inventory_service.model.StockReservation;
import com.vikas.inventory_service.repository.InventoryRepository;
import com.vikas.inventory_service.repository.ProcessedEventsRepository;
import com.vikas.inventory_service.repository.StockReservationRepository;
import com.vikas.shared.events.InventoryEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private KafkaTemplate<String, InventoryEvent> kafkaTemplate;

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private StockReservationRepository reservationRepository;

    @Mock
    private ProcessedEventsRepository processedEventsRepository;

    private InventoryService inventoryService;

    @BeforeEach
    void setUp() {
        inventoryService = new InventoryService(
                kafkaTemplate, inventoryRepository, reservationRepository, processedEventsRepository);

        ReflectionTestUtils.setField(inventoryService, "inventoryReservedTopic", "inventory.reserved");
        ReflectionTestUtils.setField(inventoryService, "inventoryInsufficientTopic", "inventory.insufficient");
    }

    @Test
    void publishSameMessageTwice_shouldReserveStockOnlyOnce() {
        String orderId = "test-order-456";
        PaymentProcessedEvent event = new PaymentProcessedEvent(orderId, "pay-123", 1L, 2, 200.0);

        Item item = new Item();
        item.setProductId(1L);
        item.setQuantity(10);
        item.setName("Test Product");
        item.setPrice(100.0);

        when(reservationRepository.existsByOrderId(orderId))
                .thenReturn(false)
                .thenReturn(true);
        when(inventoryRepository.findByProductId(1L)).thenReturn(Optional.of(item));
        when(processedEventsRepository.findById(orderId)).thenReturn(Optional.empty());

        inventoryService.onPaymentProcessed(event);
        inventoryService.onPaymentProcessed(event);

        verify(reservationRepository, times(1)).save(any(StockReservation.class));
        verify(processedEventsRepository, times(1)).save(any(ProcessedEvents.class));
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), any(InventoryEvent.class));
    }
}
