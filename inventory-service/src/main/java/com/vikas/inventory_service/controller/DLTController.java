package com.vikas.inventory_service.controller;

import com.vikas.inventory_service.model.DLTEvent;
import com.vikas.inventory_service.model.DLTStatus;
import com.vikas.inventory_service.repository.DLTRepository;
import com.vikas.inventory_service.service.InventoryService;
import com.vikas.shared.events.PaymentProcessedEvent;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import lombok.RequiredArgsConstructor;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RequiredArgsConstructor
@RestController
@RequestMapping("/admin/dlq")
public class DLTController {

    private final DLTRepository dltRepository;
    private final InventoryService inventoryService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/v1")
    public ResponseEntity<Page<DLTEvent>> handlePaymentDLT(
            @RequestParam(defaultValue = "10") @Min(value = 1, message = "count should be more than 0...") int count,
            @RequestParam(defaultValue = "0") @Min(value = 0, message = "page should be non-negative number...") int page) {
        return ResponseEntity.ok(dltRepository.findAll(PageRequest.of(page, count)));
    }

    @GetMapping("/v2")
    public ResponseEntity<List<DLTEvent>> handleDltKafka(
            @RequestParam @Max(value = 10, message = "Count should not be more than 10") @Min(value = 1, message = "count should be more than 0...") int count) {
        return ResponseEntity.ok(inventoryService.consumeDlt(count));
    }

    @PostMapping("/push/{id}")
    public void pushEvent(@PathVariable String id) {
        DLTEvent event = dltRepository.findById(id).orElseThrow();
        PaymentProcessedEvent paymentEvent = event.processToPaymentProcessedEvent();
        kafkaTemplate.send("payment.processed", paymentEvent);
        event.setStatus(DLTStatus.REPLAYED);
        dltRepository.save(event);
    }

    @PostMapping("/discard/{id}")
    public void discardEvent(@PathVariable String id) {
        DLTEvent event = dltRepository.findById(id).orElseThrow();
        event.setStatus(DLTStatus.DISCARDED);
        dltRepository.save(event);
    }
}
