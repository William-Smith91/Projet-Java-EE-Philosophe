package com.example.monitor_service;

/** Une ligne du tableau de bord. */
public record PhilosopherSummary(
        String id,
        String state,
        long lastClock,
        long meals,
        String lastEventAt) {
}