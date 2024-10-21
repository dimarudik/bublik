package org.bublik.model;

public record PGEncryptedColumn(PGColumn pgColumn, EncryptedColumn encryptedColumn) {
}
