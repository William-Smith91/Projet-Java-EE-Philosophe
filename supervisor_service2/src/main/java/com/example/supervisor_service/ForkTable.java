package com.example.supervisor_service;

import java.util.ArrayList;
import java.util.List;

/**
 * Les fourchettes, version NAÏVE (partie 2) : aucun verrou, aucune vérification globale.
 * Le rôle du superviseur se limite à :
 *   - attribuer une fourchette quand elle est libre ;
 *   - accepter qu'on la libère.
 *
 * Failles volontaires :
 *   1. take() lit l'état puis l'écrit en deux temps : deux threads peuvent tous deux
 *      voir la fourchette libre et toutes deux la recevoir (course critique).
 *   2. release() ne vérifie pas qui libère : n'importe qui peut rendre n'importe quelle fourchette.
 *   3. rien n'empêche l'interblocage ni la famine, et une fourchette jamais rendue est perdue.
 *
 * Pour la partie 3.2 (verrou), seule cette classe sera modifiée.
 */
public class ForkTable {

    private final String[] owners;            // owners[i] == null : la fourchette i est libre
    private final long checkToActDelayMs;     // élargit la fenêtre entre vérification et écriture

    public ForkTable(int forkCount, long checkToActDelayMs) {
        this.owners = new String[forkCount];
        this.checkToActDelayMs = checkToActDelayMs;
    }

    public int size() {
        return owners.length;
    }

    public boolean isFree(int forkId) {
        return owners[forkId] == null;
    }

    /** NAÏF : lecture, puis écriture, sans verrou. */
    public boolean take(int forkId, String philosopherId) {
        if (owners[forkId] != null) {
            return false;                      // déjà prise
        }
        pause();                               // <- fenêtre où un autre thread peut passer
        owners[forkId] = philosopherId;
        return true;
    }

    /** NAÏF : accepte la libération de n'importe qui, même d'une fourchette déjà libre. */
    public void release(int forkId) {
        owners[forkId] = null;
    }

    public ForkStatus status(int forkId) {
        String owner = owners[forkId];
        return new ForkStatus(forkId, owner == null, owner);
    }

    public List<ForkStatus> snapshot() {
        List<ForkStatus> result = new ArrayList<>();
        for (int i = 0; i < owners.length; i++) {
            result.add(status(i));
        }
        return result;
    }

    private void pause() {
        if (checkToActDelayMs > 0) {
            try {
                Thread.sleep(checkToActDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}