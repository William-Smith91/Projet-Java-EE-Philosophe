package com.example.monitor_service;

import java.time.Instant;

public record PhilosopherEvent(
        String philosopherId,
        EventType type,
        long logicalClock,
        Instant timestamp) {
}