package com.vikas.payment_service.service;

import com.vikas.payment_service.model.EventStatus;
import com.vikas.payment_service.model.Payment;
import com.vikas.payment_service.model.PaymentStatus;
import com.vikas.payment_service.model.ProcessedEvents;
import com.vikas.payment_service.repository.PaymentRepository;
import com.vikas.payment_service.repository.ProcessedEventsRepository;
import com.vikas.shared.events.OrderCreatedEvent;
import com.vikas.shared.events.PaymentFailedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;

@RequiredArgsConstructor
@Component
@Slf4j
public class KafkaService {

        private final KafkaTemplate<String, Object> kafkaTemplate;
        private final PaymentRepository paymentRepository;
        private final ProcessedEventsRepository processedEventsRepository;

        @KafkaListener(topics = "order.created", groupId = "payment-service")
        public void onOrderCreated(OrderCreatedEvent event) {
                log.info(
                                "Order received for payment processing: orderId={}, productId={}, qty={}",
                                event.getOrderId(),
                                event.getProductId(),
                                event.getQuantity());
                processPayment(event);
        }

        @RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 5000))
        public void processPayment(OrderCreatedEvent event) {
                Optional<ProcessedEvents> processedKey = processedEventsRepository.findById(event.getOrderId());
                if (processedKey.isPresent()) {
                        log.warn(
                                        "Event has already been processed for order {} with orderId {}",
                                        event,
                                        event.getOrderId());
                        return;
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
                        PaymentProcessedEvent processed = new PaymentProcessedEvent(
                                        event.getOrderId(),
                                        paymentId,
                                        event.getProductId(),
                                        event.getQuantity(),
                                        amount);
                        kafkaTemplate.send("payment.processed", event.getOrderId(), processed);
                        processedEventsRepository.save(
                                        new ProcessedEvents(event.getOrderId(), EventStatus.PAYMENT_PROCESSED));
                        log.info("Payment successful: orderId={}, paymentId={}", event.getOrderId(), paymentId);
                } else {
                        PaymentFailedEvent failed = new PaymentFailedEvent(event.getOrderId(), paymentId,
                                        "PAYMENT_DECLINED");
                        kafkaTemplate.send("payment.failed", event.getOrderId(), failed);
                        processedEventsRepository.save(
                                        new ProcessedEvents(event.getOrderId(), EventStatus.PAYMENT_FAILED));
                        log.info("Payment declined: orderId={}, paymentId={}", event.getOrderId(), paymentId);
                }
        }

        @DltHandler
        public void listenDlt(OrderCreatedEvent event) {
                kafkaTemplate.send("order.dlt", event);
                log.info("Event added to DLT for paymentEvent {}", event);
        }
}
