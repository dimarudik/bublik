package org.bublik.core.service;

public interface NameSyntaxService {
    default boolean isCaseSensitiveWord(String word) {
        boolean bP = (word.charAt(0) == '"' && word.charAt(word.length() - 1) == '"');
        boolean bU = word.toUpperCase().equals(word);
        boolean bL = word.toLowerCase().equals(word);
        return bP || (!bU && !bL);
    }

    default String getWordWithoutQuotes(String word) {
        return word.replaceAll("^\"|\"$", "");
    }
}
