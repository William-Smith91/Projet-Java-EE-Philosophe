package com.example.philosopher_service;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Un philosophe réactif qui exécute indépendamment une boucle infinie :
 *
 *   Penser -> Avoir faim -> Manger -> Penser -> ...
 *
 * Version « philosophes gâtés » : aucune ressource n'est partagée, donc passer de
 * « avoir faim » à « manger » ne demande aucune attente. Dans les parties suivantes
 * (superviseur, verrou, jetons), c'est dans getHungry() que l'on obtiendra les fourchettes,
 * et dans backToThinking() qu'on les rendra.
 *
 * Chaque événement porte l'horloge logique locale (un compteur propre à ce philosophe).
 */
@Component
public class Philosopher {

    /** Les états du cycle. */
    public enum State { THINKING, HUNGRY, EATING }

    private static final Logger log = LoggerFactory.getLogger(Philosopher.class);

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final String id;
    private final String topic;
    private final int thinkMin, thinkMax, hungryMax, eatMin, eatMax;

    private volatile State state = State.THINKING;
    private long clock = 0;               // horloge logique locale
    private volatile boolean running;
    private Thread lifeThread;

    public Philosopher(KafkaTemplate<String, String> kafka,
                       JsonMapper json,
                       @Value("${philosopher.id:}") String configuredId,
                       @Value("${philosopher.topic}") String topic,
                       @Value("${philosopher.think-min-ms}") int thinkMin,
                       @Value("${philosopher.think-max-ms}") int thinkMax,
                       @Value("${philosopher.hungry-max-ms}") int hungryMax,
                       @Value("${philosopher.eat-min-ms}") int eatMin,
                       @Value("${philosopher.eat-max-ms}") int eatMax) {
        this.kafka = kafka;
        this.json = json;
        this.id = configuredId.isBlank()
                ? "philosophe-" + UUID.randomUUID().toString().substring(0, 4)
                : configuredId;
        this.topic = topic;
        this.thinkMin = thinkMin;
        this.thinkMax = thinkMax;
        this.hungryMax = hungryMax;
        this.eatMin = eatMin;
        this.eatMax = eatMax;
    }

    public String getId() {
        return id;
    }

    public State getState() {
        return state;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        running = true;
        lifeThread = Thread.ofVirtual().name("philosopher-" + id).start(this::live);
        log.info("{} se met à table", id);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (lifeThread != null) {
            lifeThread.interrupt();
        }
    }

    /** La boucle infinie du philosophe : les 4 étapes de l'énoncé. */
    private void live() {
        try {
            while (running) {
                think();            // 1. Penser
                getHungry();        // 2. Avoir faim
                eat();              // 3. Manger
                backToThinking();   // 4. Reprendre la pensée
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** 1. Penser : le philosophe réfléchit pendant un temps aléatoire (aucun événement). */
    private void think() throws InterruptedException {
        state = State.THINKING;
        sleepBetween(thinkMin, thinkMax);
    }

    /** 2. Avoir faim : il souhaite manger. */
    private void getHungry() throws InterruptedException {
        state = State.HUNGRY;
        emit(EventType.HUNGRY);
        // Pas de fourchette à attendre ici. Ce court délai rend seulement l'état visible
        // sur le tableau de bord (mettre hungry-max-ms à 0 pour le supprimer).
        sleepBetween(0, hungryMax);
    }

    /** 3. Manger : il mange pendant un temps aléatoire. */
    private void eat() throws InterruptedException {
        state = State.EATING;
        emit(EventType.EATING);
        sleepBetween(eatMin, eatMax);
    }

    /** 4. Reprendre la pensée : fin du repas (DONE), retour à la réflexion. */
    private void backToThinking() {
        state = State.THINKING;
        emit(EventType.DONE);
    }

    private void emit(EventType type) {
        clock++;
        PhilosopherEvent event = new PhilosopherEvent(id, type, clock, Instant.now());
        // la clé du message = id du philosophe : ses événements restent dans l'ordre
        kafka.send(topic, id, json.writeValueAsString(event));
        log.info("{} -> {} (horloge {})", id, type, clock);
    }

    private static void sleepBetween(int minMs, int maxMs) throws InterruptedException {
        Thread.sleep(ThreadLocalRandom.current().nextLong(minMs, maxMs + 1));
    }
}
