package com.example.monitor_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/** Reçoit chaque événement publié par les philosophes. */
@Component
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final JsonMapper json;
    private final EventStore store;

    public EventConsumer(JsonMapper json, EventStore store) {
        this.json = json;
        this.store = store;
    }

    @KafkaListener(topics = "${monitor.topic}")
    public void onMessage(String payload) {
        try {
            store.add(json.readValue(payload, PhilosopherEvent.class));
        } catch (RuntimeException e) {
            log.warn("Message illisible ignoré : {}", payload);
        }
    }
}