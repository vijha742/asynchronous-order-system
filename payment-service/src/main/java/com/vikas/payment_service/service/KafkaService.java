package com.vikas.payment_service.service;

import com.vikas.shared.events.*;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Random;

@RequiredArgsConstructor
@Component
@Slf4j
public class KafkaService {

    private final KafkaTemplate<Long, PaymentEvent> kafkaTemplate;

    @KafkaListener(topics = "order.created", groupId = "payment-service")
    public void listen(OrderCreatedEvent event) {
        log.info("Event read: {}", event);
        processPayment(event);
    }

    public boolean processPayment(OrderCreatedEvent event) {
        boolean isPaymentSuccessful = true;
        Random random = new Random();
        if (random.nextBoolean()) {
            isPaymentSuccessful = false;
        }

        if (isPaymentSuccessful) {
            PaymentEvent paymentProcessedEvent = new PaymentProcessedEvent(event.getOrderId(), random.nextLong());
            kafkaTemplate.send("payment.success", paymentProcessedEvent);
            log.info("Payment successful: {}", event);
            return true;
        } else {
            PaymentEvent paymentFailedEvent = new PaymentFailedEvent(event.getOrderId(), random.nextLong());
            kafkaTemplate.send("payment.failure", paymentFailedEvent);
            log.info("Payment failed: {}", event);
            return false;
        }
    }
}
