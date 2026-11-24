package com.example.philosopher_service;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
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
 * Philosophe sous la supervision d'un superviseur (partie 2).
 *
 *   Penser -> Avoir faim -> Manger -> Penser -> ...
 *
 * Le philosophe i a besoin de la fourchette i puis de la fourchette (i+1) mod N.
 * Pour chacune, il demande au superviseur si elle est libre, puis la demande.
 * Le superviseur ne contrôle rien d'autre : c'est ce qui rend l'interblocage possible.
 */
@Component
public class Philosopher {

    public enum State { THINKING, HUNGRY, EATING }

    private static final Logger log = LoggerFactory.getLogger(Philosopher.class);

    private final KafkaTemplate<String, String> kafka;
    private final JsonMapper json;
    private final SupervisorClient supervisor;
    private final String id;
    private final String topic;
    private final int firstFork, secondFork;
    private final int thinkMin, thinkMax, eatMin, eatMax, betweenForksMs, pollMs;

    private volatile State state = State.THINKING;
    private long clock = 0;               // horloge logique locale
    private volatile boolean running;
    private Thread lifeThread;

    public Philosopher(KafkaTemplate<String, String> kafka,
                       JsonMapper json,
                       SupervisorClient supervisor,
                       @Value("${philosopher.id:}") String configuredId,
                       @Value("${philosopher.index}") int index,
                       @Value("${philosopher.count}") int count,
                       @Value("${philosopher.topic}") String topic,
                       @Value("${philosopher.think-min-ms}") int thinkMin,
                       @Value("${philosopher.think-max-ms}") int thinkMax,
                       @Value("${philosopher.eat-min-ms}") int eatMin,
                       @Value("${philosopher.eat-max-ms}") int eatMax,
                       @Value("${philosopher.between-forks-ms}") int betweenForksMs,
                       @Value("${philosopher.poll-ms}") int pollMs) {
        this.kafka = kafka;
        this.json = json;
        this.supervisor = supervisor;
        this.id = configuredId.isBlank() ? "philosophe-" + index : configuredId;
        this.firstFork = index;
        this.secondFork = (index + 1) % count;
        this.topic = topic;
        this.thinkMin = thinkMin;
        this.thinkMax = thinkMax;
        this.eatMin = eatMin;
        this.eatMax = eatMax;
        this.betweenForksMs = betweenForksMs;
        this.pollMs = pollMs;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        running = true;
        lifeThread = Thread.ofVirtual().name("philosopher-" + id).start(this::live);
        log.info("{} se met à table (fourchettes {} et {})", id, firstFork, secondFork);
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (lifeThread != null) {
            lifeThread.interrupt();
        }
    }

    /** La boucle infinie du philosophe. */
    private void live() {
        try {
            while (running) {
                think();            // 1. Penser
                getHungry();        // 2. Avoir faim (obtenir les deux fourchettes)
                eat();              // 3. Manger
                backToThinking();   // 4. Rendre les fourchettes et reprendre la pensée
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void think() throws InterruptedException {
        state = State.THINKING;
        sleepBetween(thinkMin, thinkMax);
    }

    /** Il a faim : il reste HUGRY tant qu'il n'a pas obtenu ses deux fourchettes. */
    private void getHungry() throws InterruptedException {
        state = State.HUNGRY;
        emit(EventType.HUNGRY);
        acquire(firstFork);
        Thread.sleep(betweenForksMs);   // la seconde demande ne part pas instantanément
        acquire(secondFork);
    }

    private void eat() throws InterruptedException {
        state = State.EATING;
        emit(EventType.EATING);
        sleepBetween(eatMin, eatMax);
    }

    private void backToThinking() throws InterruptedException {
        release(firstFork);
        release(secondFork);
        state = State.THINKING;
        emit(EventType.DONE);
    }

    /**
     * Obtenir une fourchette : demander si elle est libre, puis la demander.
     * Si elle est occupée, on attend pollMs puis on redemande (attente active).
     */
    private void acquire(int forkId) throws InterruptedException {
        while (running) {
            try {
                if (supervisor.isFree(forkId) && supervisor.take(forkId, id)) {
                    log.info("{} a obtenu la fourchette {}", id, forkId);
                    return;
                }
            } catch (RuntimeException e) {
                log.warn("{} : superviseur injoignable ({})", id, e.getMessage());
            }
            Thread.sleep(pollMs);
        }
        throw new InterruptedException("arrêt du philosophe");
    }

    /** Rendre une fourchette (on réessaie si le superviseur ne répond pas). */
    private void release(int forkId) throws InterruptedException {
        while (running) {
            try {
                supervisor.release(forkId);
                log.info("{} a rendu la fourchette {}", id, forkId);
                return;
            } catch (RuntimeException e) {
                log.warn("{} : impossible de rendre la fourchette {} ({})", id, forkId, e.getMessage());
            }
            Thread.sleep(pollMs);
        }
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