package com.vikas.payment_service.service;

import com.vikas.payment_service.model.Payment;
import com.vikas.payment_service.model.PaymentStatus;
import com.vikas.payment_service.model.ProcessedEvents;
import com.vikas.payment_service.repository.PaymentRepository;
import com.vikas.payment_service.repository.ProcessedEventsRepository;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.OrderCreatedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentProcessingService {

    private final PaymentRepository paymentRepository;
    private final ProcessedEventsRepository processedEventsRepository;

    public enum PaymentResultType {
        SUCCESS,
        FAILED,
        ALREADY_PROCESSED
    }

    public record PaymentResult(PaymentResultType type, String paymentId, double amount) {
    }

    @Transactional
    public PaymentResult processPayment(OrderCreatedEvent event) {
        Optional<ProcessedEvents> processedKey = processedEventsRepository.findById(event.getOrderId());
        if (processedKey.isPresent()) {
            log.warn("Event has already been processed for orderId {}", event.getOrderId());
            return new PaymentResult(PaymentResultType.ALREADY_PROCESSED, null, 0.0);
        }

        try {
            processedEventsRepository.saveAndFlush(new ProcessedEvents(event.getOrderId()));
        } catch (org.springframework.dao.DataIntegrityViolationException ex) {
            log.warn(
                    "Duplicate payment event detected via DB constraint: orderId={}",
                    event.getOrderId());
            return new PaymentResult(PaymentResultType.ALREADY_PROCESSED, null, 0.0);
        }

        String paymentId = UUID.randomUUID().toString();
        boolean success = new Random().nextInt(100) < 80;
        double amount = event.getQuantity() * 100.0;

        Payment payment = new Payment();
        payment.setPaymentId(paymentId);
        payment.setOrderId(event.getOrderId());
        payment.setProductId(event.getProductId());
        payment.setQuantity(event.getQuantity());
        payment.setAmount(amount);
        payment.setStatus(success ? PaymentStatus.CONFIRMED : PaymentStatus.FAILED);
        paymentRepository.save(payment);

        if (success) {
            log.info("Payment successful: orderId={}, paymentId={}", event.getOrderId(), paymentId);
            return new PaymentResult(PaymentResultType.SUCCESS, paymentId, amount);
        } else {
            log.info("Payment declined: orderId={}, paymentId={}", event.getOrderId(), paymentId);
            return new PaymentResult(PaymentResultType.FAILED, paymentId, amount);
        }
    }

    @Transactional
    public Payment processRefund(InventoryInsufficientEvent event) {
        Payment payment = paymentRepository.findById(event.getPaymentId())
                .orElseThrow(() -> new NoSuchElementException(
                        "Cannot refund: payment not found for paymentId=" + event.getPaymentId()));

        payment.setStatus(PaymentStatus.REFUNDED);
        Payment saved = paymentRepository.save(payment);
        log.info("Refund processed: orderId={}, paymentId={}", payment.getOrderId(), payment.getPaymentId());
        return saved;
    }
}
