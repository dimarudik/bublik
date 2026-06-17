package dev.bublik.clickhouse.model;

import dev.bublik.clickhouse.service.ColumnTransfer;

public record TransferPlan(ColumnTransfer[] transfers, String[] sourceColumnNames, int[] chIndices) {
}
