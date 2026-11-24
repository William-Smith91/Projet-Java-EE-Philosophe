package com.example.philosopher_service;

/** Réponse du superviseur pour GET /forks/{id}. */
public record ForkStatus(int id, boolean free, String owner) {
}
