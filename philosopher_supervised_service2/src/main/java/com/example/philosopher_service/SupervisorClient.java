package com.example.philosopher_service;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/** Appels HTTP du philosophe vers le superviseur. */
@Component
public class SupervisorClient {

    private final RestClient rest;

    public SupervisorClient(@Value("${supervisor.url}") String url) {
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(1)).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(3));
        this.rest = RestClient.builder().baseUrl(url).requestFactory(factory).build();
    }

    /** "La fourchette est-elle disponible ?" */
    public boolean isFree(int forkId) {
        ForkStatus status = rest.get().uri("/forks/{id}", forkId).retrieve().body(ForkStatus.class);
        return status != null && status.free();
    }

    /** "Je demande cette fourchette" : true si le superviseur me l'attribue. */
    public boolean take(int forkId, String philosopherId) {
        Boolean granted = rest.post()
                .uri("/forks/{id}/take?philosopher={p}", forkId, philosopherId)
                .retrieve()
                .body(Boolean.class);
        return Boolean.TRUE.equals(granted);
    }

    /** "Je rends cette fourchette". */
    public void release(int forkId) {
        rest.post().uri("/forks/{id}/release", forkId).retrieve().toBodilessEntity();
    }
}
