package com.vikas.order_service.service;

import com.vikas.order_service.model.OrderPollerEvent;
import com.vikas.order_service.model.PollerStatus;
import com.vikas.order_service.repository.Outbox;
import com.vikas.shared.events.OrderCreatedEvent;

import jakarta.transaction.Transactional;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
@Slf4j
public class ScheduledService {

    private final Outbox outbox;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    // NOTE: Work pending on retry/backoff logic for Poller so that resources aren't
    // wasted...
    // during infra failures.
    @Scheduled(fixedDelay = 5000)
    public void processOutbox() {
        Page<OrderPollerEvent> pollerObjects = outbox.findByStatus(
                PollerStatus.PENDING, PageRequest.of(0, 50, Sort.by("createdAt")));
        for (OrderPollerEvent order : pollerObjects) {
            processItem(order);
        }
    }

    @Transactional
    public void processItem(OrderPollerEvent order) {

        OrderCreatedEvent event = new OrderCreatedEvent(
                order.getOrderId(), order.getProductId(), order.getQuantity());
        try {
            kafkaTemplate.send("order.created", order.getOrderId(), event).get();
            order.setStatus(PollerStatus.PROCESSED);
            outbox.save(order);
            log.info(
                    "Order created and event published: orderId={}, productId={}, quantity={}",
                    order.getOrderId(),
                    order.getProductId(),
                    order.getQuantity());
        } catch (InterruptedException e) {
            log.warn(
                    "Cannot verify event publish result for Order with orderId {}",
                    order.getOrderId());
        } catch (ExecutionException e) {
            log.error(
                    "Order couldn't get published for order with orderId : {}", order.getOrderId());
        }
    }
}
