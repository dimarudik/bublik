package dev.bublik.core.model;

public record ColumnValue<V>(Column sourceColumn, Column targetColumn, V value) {
}
