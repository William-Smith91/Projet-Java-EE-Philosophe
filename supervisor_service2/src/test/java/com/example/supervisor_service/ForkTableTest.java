package com.example.supervisor_service;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Tests unitaires (comportement normal) puis tests qui DOCUMENTENT les failles de la version
 * naïve : ils sont verts parce qu'ils vérifient que la faille se produit bien.
 * Avec le verrou de la partie 3.2, il faudra inverser les assertions des tests "faille_".
 */
class ForkTableTest {

    // ---------- comportement normal ----------

    @Test
    void uneFourchetteLibrePeutEtrePrise() {
        ForkTable table = new ForkTable(5, 0);
        assertThat(table.take(0, "platon")).isTrue();
        assertThat(table.isFree(0)).isFalse();
    }

    @Test
    void uneFourchettePriseEstRefusee() {
        ForkTable table = new ForkTable(5, 0);
        table.take(0, "platon");
        assertThat(table.take(0, "kant")).isFalse();
    }

    @Test
    void uneFourchetteLibereePeutEtreReprise() {
        ForkTable table = new ForkTable(5, 0);
        table.take(0, "platon");
        table.release(0);
        assertThat(table.take(0, "kant")).isTrue();
    }

    // ---------- failles de la version naïve ----------

    /** Faille : le superviseur ne vérifie pas qui libère la fourchette. */
    @Test
    void faille_nImporteQuiPeutLibererLaFourchetteDunAutre() {
        ForkTable table = new ForkTable(5, 0);
        table.take(0, "platon");
        table.release(0);                      // appelé par un autre philosophe : rien ne l'empêche
        assertThat(table.isFree(0)).isTrue();  // platon croit encore la tenir
    }

    /** Faille : course critique, la même fourchette est attribuée à plusieurs philosophes. */
    @Test
    void faille_laMemeFourchetteEstAttribueeAPlusieursPhilosophes() throws Exception {
        ForkTable table = new ForkTable(5, 20);   // 20 ms entre vérification et écriture
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger granted = new AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final String name = "philosophe-" + i;
            pool.submit(() -> {
                ready.countDown();
                go.await();                       // tous partent en même temps
                if (table.take(0, name)) {
                    granted.incrementAndGet();
                }
                return null;
            });
        }
        ready.await();
        go.countDown();
        pool.shutdown();
        assertThat(pool.awaitTermination(5, TimeUnit.SECONDS)).isTrue();

        // Une fourchette ne devrait être attribuée qu'une fois : ici, plusieurs philosophes l'ont obtenue.
        assertThat(granted.get()).isGreaterThan(1);
    }

    /** Faille : interblocage, chacun tient sa première fourchette et attend la seconde. */
    @Test
    void faille_interblocageQuandChacunTientSaPremiereFourchette() throws Exception {
        int n = 5;
        ForkTable table = new ForkTable(n, 0);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CyclicBarrier tousTiennentLaPremiere = new CyclicBarrier(n);
        AtomicInteger ontMange = new AtomicInteger();

        for (int i = 0; i < n; i++) {
            final int moi = i;
            pool.submit(() -> {
                table.take(moi, "philosophe-" + moi);           // première fourchette : la mienne
                tousTiennentLaPremiere.await();                 // tout le monde en tient une
                long limite = System.currentTimeMillis() + 500;
                while (System.currentTimeMillis() < limite) {   // seconde : celle du voisin
                    if (table.take((moi + 1) % n, "philosophe-" + moi)) {
                        ontMange.incrementAndGet();
                        break;
                    }
                    Thread.sleep(10);
                }
                return null;
            });
        }
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();

        // Personne n'a pu obtenir sa seconde fourchette : attente circulaire.
        assertThat(ontMange.get()).isZero();
    }
}
