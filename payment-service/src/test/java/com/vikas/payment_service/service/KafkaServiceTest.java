package com.vikas.payment_service.service;

import com.vikas.payment_service.service.PaymentProcessingService.PaymentResult;
import com.vikas.payment_service.service.PaymentProcessingService.PaymentResultType;
import com.vikas.shared.events.OrderCreatedEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaServiceTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private PaymentProcessingService paymentProcessingService;

    private KafkaService kafkaService;

    @BeforeEach
    void setUp() {
        kafkaService = new KafkaService(kafkaTemplate, paymentProcessingService);
    }

    @Test
    void whenPaymentSuccessful_shouldPublishProcessedEvent() {
        String orderId = "test-order-123";
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, 1L, 2, System.currentTimeMillis());
        PaymentResult result = new PaymentResult(PaymentResultType.SUCCESS, "pay-123", 200.0);

        when(paymentProcessingService.processPayment(event)).thenReturn(result);

        kafkaService.onOrderCreated(event);

        verify(kafkaTemplate, times(1)).send(eq("payment.processed"), eq(orderId), any());
        verify(kafkaTemplate, never()).send(eq("payment.failed"), anyString(), any());
    }

    @Test
    void whenPaymentFailed_shouldPublishFailedEvent() {
        String orderId = "test-order-123";
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, 1L, 2, System.currentTimeMillis());
        PaymentResult result = new PaymentResult(PaymentResultType.FAILED, "pay-123", 200.0);

        when(paymentProcessingService.processPayment(event)).thenReturn(result);

        kafkaService.onOrderCreated(event);

        verify(kafkaTemplate, times(1)).send(eq("payment.failed"), eq(orderId), any());
        verify(kafkaTemplate, never()).send(eq("payment.processed"), anyString(), any());
    }

    @Test
    void whenAlreadyProcessed_shouldSkipPublishing() {
        String orderId = "test-order-123";
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, 1L, 2, System.currentTimeMillis());
        PaymentResult result = new PaymentResult(PaymentResultType.ALREADY_PROCESSED, null, 0.0);

        when(paymentProcessingService.processPayment(event)).thenReturn(result);

        kafkaService.onOrderCreated(event);

        verify(kafkaTemplate, never()).send(anyString(), anyString(), any());
    }
}
