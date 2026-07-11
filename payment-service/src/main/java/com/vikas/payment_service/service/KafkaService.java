package com.vikas.payment_service.service;

import com.vikas.payment_service.model.Payment;
import com.vikas.payment_service.service.PaymentProcessingService.PaymentResult;
import com.vikas.shared.events.InventoryInsufficientEvent;
import com.vikas.shared.events.OrderCreatedEvent;
import com.vikas.shared.events.PaymentFailedEvent;
import com.vikas.shared.events.PaymentProcessedEvent;
import com.vikas.shared.events.PaymentRefundedEvent;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@RequiredArgsConstructor
@Component
@Slf4j
public class KafkaService {

        private final KafkaTemplate<String, Object> kafkaTemplate;
        private final PaymentProcessingService paymentProcessingService;

        @RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 5000))
        @KafkaListener(topics = "order.created", groupId = "payment-service")
        public void onOrderCreated(OrderCreatedEvent event) {
                log.info(
                                "Order received for payment processing: orderId={}, productId={}, qty={}",
                                event.getOrderId(),
                                event.getProductId(),
                                event.getQuantity());

                PaymentResult result = paymentProcessingService.processPayment(event);

                if (result.type() == PaymentProcessingService.PaymentResultType.SUCCESS) {
                        PaymentProcessedEvent processed = new PaymentProcessedEvent(
                                        event.getOrderId(),
                                        result.paymentId(),
                                        event.getProductId(),
                                        event.getQuantity(),
                                        result.amount());
                        kafkaTemplate.send("payment.processed", event.getOrderId(), processed);
                } else if (result.type() == PaymentProcessingService.PaymentResultType.FAILED) {
                        PaymentFailedEvent failed = new PaymentFailedEvent(
                                        event.getOrderId(), result.paymentId(), "PAYMENT_DECLINED");
                        kafkaTemplate.send("payment.failed", event.getOrderId(), failed);
                }
        }

        @RetryableTopic(attempts = "4", backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 5000))
        @KafkaListener(topics = "inventory.insufficient", groupId = "payment-service")
        public void refundOnFailedInventory(InventoryInsufficientEvent event) {
                Payment refundedPayment = paymentProcessingService.processRefund(event);
                PaymentRefundedEvent refundedEvent = new PaymentRefundedEvent(
                                refundedPayment.getOrderId(),
                                refundedPayment.getPaymentId(),
                                "INVENTORY_INSUFFICIENT");
                kafkaTemplate.send("payment.refunded", refundedPayment.getOrderId(), refundedEvent);
                log.info(
                                "Refund event published: orderId={}, paymentId={}",
                                refundedPayment.getOrderId(),
                                refundedPayment.getPaymentId());
        }

        @DltHandler
        public void listenDlt(OrderCreatedEvent event) {
                kafkaTemplate.send("order.dlt", event);
                log.info("Event added to DLT for paymentEvent {}", event);
        }
}
