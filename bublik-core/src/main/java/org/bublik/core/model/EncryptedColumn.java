package org.bublik.core.model;

public record EncryptedColumn(
    String targetEncColumnName,
    String sourceAadColumnName,
    String targetEncMetaColumnName
) {}
