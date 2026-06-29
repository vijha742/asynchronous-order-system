package com.vikas.payment_service.service;

import com.vikas.payment_service.model.Payment;
import com.vikas.payment_service.model.PaymentStatus;
import com.vikas.payment_service.repository.PaymentRepository;
import com.vikas.shared.events.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

@RequiredArgsConstructor
@Component
@Slf4j
public class KafkaService {

    private final KafkaTemplate<Long, PaymentEvent> kafkaTemplate;
    private final PaymentRepository paymentRepository;

    @KafkaListener(topics = "order.created", groupId = "payment-service")
    public void listen(OrderCreatedEvent event) {
        log.info("Event read: {}", event);
        processPayment(event);
    }

    public void processPayment(OrderCreatedEvent event) {
        boolean isPaymentSuccessful = false;
        Random random = new Random();
        Long paymentId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        if (random.nextBoolean()) {
            isPaymentSuccessful = true;
        }

        Payment payment = new Payment();
        payment.setPaymentId(paymentId);
        payment.setOrderId(event.getOrderId());
        payment.setCreatedAt(System.currentTimeMillis());
        payment.setUpdatedAt(System.currentTimeMillis());
        payment.setAmount(event.getQuantity() * 100.0);
        payment.setStatus(isPaymentSuccessful ? PaymentStatus.CONFIRMED : PaymentStatus.FAILED);
        paymentRepository.save(payment);

        if (isPaymentSuccessful) {
            PaymentEvent paymentProcessedEvent = new PaymentProcessedEvent(event.getOrderId(), paymentId);
            publishPaymentEvent(paymentProcessedEvent);
        } else {
            PaymentEvent paymentFailedEvent = new PaymentFailedEvent(event.getOrderId(), paymentId);
            publishPaymentEvent(paymentFailedEvent);
        }
    }

    public void publishPaymentEvent(PaymentEvent event) {
        if (event instanceof PaymentProcessedEvent) {
            kafkaTemplate.send("payment.processed", event);
            log.info("Payment successful: {}", event);
        } else if (event instanceof PaymentFailedEvent) {
            kafkaTemplate.send("payment.failed", event);
            log.info("Payment failed: {}", event);
        }
    }
}
