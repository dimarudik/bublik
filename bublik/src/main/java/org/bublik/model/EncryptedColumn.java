package org.bublik.model;

public record EncryptedColumn(
    String targetEncColumnName,
    String sourceAadColumnName,
    String targetEncMetaColumnName
) {}
