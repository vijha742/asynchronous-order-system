package com.vikas.order_service.model;

import java.util.List;

public record ErrorResponse(String message, Long timestamp, List<String> details) {
}
