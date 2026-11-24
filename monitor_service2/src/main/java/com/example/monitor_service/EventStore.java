package com.example.monitor_service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.springframework.stereotype.Service;

/**
 * Mémoire du moniteur : un historique par philosophe.
 * Chaque historique est un TreeMap trié par horloge logique : même si deux événements
 * arrivent dans le désordre, l'historique reconstruit est dans le bon ordre.
 * Les horloges de philosophes différents ne se comparent pas : il n'y a pas d'ordre global.
 *
 * Les méthodes sont "synchronized" car le thread Kafka écrit pendant que le thread web lit.
 */
@Service
public class EventStore {

    private final Map<String, TreeMap<Long, PhilosopherEvent>> histories = new HashMap<>();

    public synchronized void add(PhilosopherEvent event) {
        histories.computeIfAbsent(event.philosopherId(), id -> new TreeMap<>())
                .put(event.logicalClock(), event);
    }

    public synchronized List<PhilosopherSummary> summaries() {
        List<PhilosopherSummary> result = new ArrayList<>();
        for (Map.Entry<String, TreeMap<Long, PhilosopherEvent>> entry : histories.entrySet()) {
            TreeMap<Long, PhilosopherEvent> history = entry.getValue();
            PhilosopherEvent last = history.lastEntry().getValue();
            long meals = history.values().stream().filter(e -> e.type() == EventType.DONE).count();
            String state = switch (last.type()) {
                case HUNGRY -> "HUNGRY";
                case EATING -> "EATING";
                case DONE -> "THINKING";
            };
            result.add(new PhilosopherSummary(entry.getKey(), state, last.logicalClock(),
                    meals, EventView.format(last.timestamp())));
        }
        result.sort((a, b) -> a.id().compareTo(b.id()));
        return result;
    }

    /** Historique d'un philosophe, du plus récent au plus ancien (200 derniers). */
    public synchronized List<EventView> history(String id) {
        TreeMap<Long, PhilosopherEvent> history = histories.get(id);
        if (history == null) {
            return List.of();
        }
        return history.descendingMap().values().stream()
                .limit(200)
                .map(EventView::from)
                .toList();
    }
}