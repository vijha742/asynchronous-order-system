package com.vikas.payment_service.service;

import com.vikas.payment_service.model.EventStatus;
import com.vikas.payment_service.model.Payment;
import com.vikas.payment_service.model.ProcessedEvents;
import com.vikas.payment_service.repository.PaymentRepository;
import com.vikas.payment_service.repository.ProcessedEventsRepository;
import com.vikas.shared.events.OrderCreatedEvent;
import com.vikas.shared.events.PaymentEvent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaServiceTest {

    @Mock
    private KafkaTemplate<String, PaymentEvent> kafkaTemplate;

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private ProcessedEventsRepository processedEventsRepository;

    private KafkaService kafkaService;

    @BeforeEach
    void setUp() {
        kafkaService = new KafkaService(kafkaTemplate, paymentRepository, processedEventsRepository);
    }

    @Test
    void publishSameMessageTwice_shouldProcessPaymentOnlyOnce() {
        String orderId = "test-order-123";
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, 1L, 2, System.currentTimeMillis());

        when(processedEventsRepository.findById(orderId))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(new ProcessedEvents(orderId, EventStatus.PAYMENT_PROCESSED)));

        kafkaService.onOrderCreated(event);
        kafkaService.onOrderCreated(event);

        verify(paymentRepository, times(1)).save(any(Payment.class));
        verify(processedEventsRepository, times(1)).save(any(ProcessedEvents.class));
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), any(PaymentEvent.class));
    }
}
