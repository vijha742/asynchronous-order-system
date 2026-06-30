package com.vikas.inventory_service.service;

import com.vikas.inventory_service.model.Item;
import com.vikas.inventory_service.repository.InventoryRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DataInitializer implements ApplicationRunner {

    private final InventoryRepository inventoryRepository;

    @Override
    public void run(ApplicationArguments args) {
        if (inventoryRepository.count() == 0) {
            inventoryRepository.save(buildItem(1L, "Widget A", 100, 9.99));
            inventoryRepository.save(buildItem(2L, "Widget B", 50, 19.99));
            inventoryRepository.save(buildItem(3L, "Widget C", 200, 4.99));
            log.info("Inventory seeded with 3 products (productId 1, 2, 3)");
        } else {
            log.info(
                    "Inventory already has {} products — skipping seed",
                    inventoryRepository.count());
        }
    }

    private Item buildItem(Long productId, String name, int quantity, double price) {
        Item item = new Item();
        item.setProductId(productId);
        item.setName(name);
        item.setQuantity(quantity);
        item.setPrice(price);
        return item;
    }
}
