package org.example.service;

public interface SQLSyntaxService {
    default boolean isCaseSensitiveWord(String word){
        return word.charAt(0) == '"' && word.charAt(word.length() - 1) == '"';
    }

    default String getWordWithoutQuotes(String word) {
        return word.replaceAll("^\"|\"$", "");
    }
}
