package org.bublik.model;

public record EncryptedEntity (
    String encryptedColumnName,
    String encryptionMetadataColumnName
) {}
