package dev.bublik.postgres.model;

public record PgIntervalComponents(int months, int days, long microseconds) {
}
