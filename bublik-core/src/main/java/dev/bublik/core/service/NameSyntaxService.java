package dev.bublik.core.service;

import dev.bublik.core.constants.PGKeywords;

public interface NameSyntaxService {
    default boolean isReservedWord(String word) {
        return PGKeywords.contains(word.toUpperCase());
    }

    default boolean isCaseSensitiveWord(String word) {
        boolean bP = (word.charAt(0) == '"' && word.charAt(word.length() - 1) == '"');
        boolean bU = word.toUpperCase().equals(word);
        boolean bL = word.toLowerCase().equals(word);
        return bP || (!bU && !bL);
    }

    default boolean isOracleCaseSensitiveWord(String word) {
        boolean bU = word.toUpperCase().equals(word);
        return !bU;
    }

    default String getWordWithoutQuotes(String word) {
        return word.replaceAll("^\"|\"$", "");
    }
}
