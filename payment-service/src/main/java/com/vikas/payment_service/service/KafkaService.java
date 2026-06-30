package com.vikas.payment_service.service;

import com.vikas.payment_service.model.Payment;
import com.vikas.payment_service.model.PaymentStatus;
import com.vikas.payment_service.repository.PaymentRepository;
import com.vikas.shared.events.OrderCreatedEvent;
import com.vikas.shared.events.PaymentEvent;
import com.vikas.shared.events.PaymentFailedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.UUID;

@RequiredArgsConstructor
@Component
@Slf4j
public class KafkaService {

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final PaymentRepository paymentRepository;

    @KafkaListener(topics = "order.created", groupId = "payment-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.info("Order received for payment processing: orderId={}, productId={}, qty={}",
                event.getOrderId(), event.getProductId(), event.getQuantity());
        processPayment(event);
    }

    private void processPayment(OrderCreatedEvent event) {
        String paymentId = UUID.randomUUID().toString();
        boolean success = new Random().nextInt(100) < 80; // 80% success rate per spec

        double amount = event.getQuantity() * 100.0;

        Payment payment = new Payment();
        payment.setPaymentId(paymentId);
        payment.setOrderId(event.getOrderId());
        payment.setProductId(event.getProductId());   // ← forwarded — needed for refund saga
        payment.setQuantity(event.getQuantity());
        payment.setAmount(amount);
        payment.setStatus(success ? PaymentStatus.CONFIRMED : PaymentStatus.FAILED);
        paymentRepository.save(payment);

        if (success) {
            PaymentProcessedEvent processed = new PaymentProcessedEvent(
                    event.getOrderId(),
                    paymentId,
                    event.getProductId(),   // ← critical: Inventory needs this to find the stock row
                    event.getQuantity(),
                    amount);
            kafkaTemplate.send("payment.processed", event.getOrderId(), processed);
            log.info("Payment successful: orderId={}, paymentId={}", event.getOrderId(), paymentId);
        } else {
            PaymentFailedEvent failed = new PaymentFailedEvent(
                    event.getOrderId(),
                    paymentId,
                    "PAYMENT_DECLINED");
            kafkaTemplate.send("payment.failed", event.getOrderId(), failed);
            log.info("Payment declined: orderId={}, paymentId={}", event.getOrderId(), paymentId);
        }
    }
}
