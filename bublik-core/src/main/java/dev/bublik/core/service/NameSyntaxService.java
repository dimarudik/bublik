package dev.bublik.core.service;

import dev.bublik.core.constants.PGKeywords;

public interface NameSyntaxService {
    default boolean isReservedWord(String word) {
        return PGKeywords.contains(word.toUpperCase());
    }

    default boolean isCaseSensitiveWord(String word) {
        if (word == null || word.isEmpty()) {
            return false;
        }

        boolean bP = word.length() >= 2 && word.startsWith("\"") && word.endsWith("\"");
        boolean startsWithDigit = Character.isDigit(word.charAt(0));
        boolean hasUpperCase = !word.toLowerCase().equals(word);

        return bP || startsWithDigit || hasUpperCase;
    }

    default boolean isOracleCaseSensitiveWord(String word) {
        boolean bU = word.toUpperCase().equals(word);
        return !bU;
    }

    default String getWordWithoutQuotes(String word) {
        return word.replaceAll("^\"|\"$", "");
    }
}
