package com.example.supervisor_service;

/** État d'une fourchette : libre ou tenue par un philosophe. */
public record ForkStatus(int id, boolean free, String owner) {
}
