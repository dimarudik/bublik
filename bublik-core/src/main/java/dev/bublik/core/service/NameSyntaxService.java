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
        boolean startsWithLetter = Character.isAlphabetic(word.charAt(0));
        boolean hasUpperCase = !word.toLowerCase().equals(word);

        return bP || startsWithDigit || hasUpperCase || !startsWithLetter;
    }

    default boolean isOracleCaseSensitiveWord(String word) {
        if (word == null || word.isEmpty()) {
            return false;
        }

        boolean hasLowerCase = !word.toUpperCase().equals(word);
        boolean startsWithLetter = Character.isAlphabetic(word.charAt(0));

        return hasLowerCase || !startsWithLetter;
    }

    default String getWordWithoutQuotes(String word) {
        return word.replaceAll("^\"|\"$", "");
    }
}
