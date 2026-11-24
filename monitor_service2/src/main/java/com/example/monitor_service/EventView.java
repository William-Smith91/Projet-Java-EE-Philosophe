package com.example.monitor_service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** Une ligne d'historique, prête à afficher. */
public record EventView(long logicalClock, String type, String time) {

    private static final DateTimeFormatter TIME =
            DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault());

    static String format(Instant instant) {
        return TIME.format(instant);
    }

    static EventView from(PhilosopherEvent e) {
        return new EventView(e.logicalClock(), e.type().name(), format(e.timestamp()));
    }
}
