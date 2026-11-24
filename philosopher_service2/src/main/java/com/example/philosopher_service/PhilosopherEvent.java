package com.example.philosopher_service;

import java.time.Instant;

/**
 * Événement envoyé sur Kafka.
 * logicalClock : horloge logique LOCALE à ce philosophe (1, 2, 3, ...).
 * timestamp    : heure réelle, uniquement pour l'affichage.
 */
public record PhilosopherEvent(
        String philosopherId,
        EventType type,
        long logicalClock,
        Instant timestamp) {
}
