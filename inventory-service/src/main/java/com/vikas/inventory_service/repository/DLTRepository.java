package com.vikas.inventory_service.repository;

import com.vikas.inventory_service.model.DLTEvent;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DLTRepository extends JpaRepository<DLTEvent, String> {}
