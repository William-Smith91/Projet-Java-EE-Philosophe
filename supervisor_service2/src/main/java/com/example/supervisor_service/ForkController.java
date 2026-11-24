package com.example.supervisor_service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

/**
 * API REST du superviseur. Il ne décide jamais si un philosophe peut manger :
 * il répond seulement fourchette par fourchette.
 */
@RestController
@RequestMapping("/forks")
public class ForkController {

    private final ForkTable table;
    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong granted = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    public ForkController(ForkTable table) {
        this.table = table;
    }

    /** État de toutes les fourchettes (pour observer le système). */
    @GetMapping
    public List<ForkStatus> all() {
        return table.snapshot();
    }

    /** Compteurs : permet de mesurer les interrogations inutiles. */
    @GetMapping("/stats")
    public Map<String, Long> stats() {
        return Map.of("requests", requests.get(), "granted", granted.get(), "refused", refused.get());
    }

    /** "La fourchette est-elle disponible ?" */
    @GetMapping("/{id}")
    public ForkStatus one(@PathVariable("id") int id) {
        requests.incrementAndGet();
        return table.status(checked(id));
    }

    /** "Je demande cette fourchette" : true si elle est attribuée, false sinon. */
    @PostMapping("/{id}/take")
    public boolean take(@PathVariable("id") int id, @RequestParam("philosopher") String philosopher) {
        requests.incrementAndGet();
        boolean ok = table.take(checked(id), philosopher);
        (ok ? granted : refused).incrementAndGet();
        return ok;
    }

    /** "Je rends cette fourchette". */
    @PostMapping("/{id}/release")
    public void release(@PathVariable("id") int id) {
        requests.incrementAndGet();
        table.release(checked(id));
    }

    private int checked(int id) {
        if (id < 0 || id >= table.size()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Fourchette inconnue");
        }
        return id;
    }
}
