package org.chekist;

import org.bublik.secure.Crypto;

public class Chekist implements Crypto<String, String, String, String, String> {
    @Override
    public String encrypt(String s, String s2) {
        return "***";
    }

    @Override
    public String decrypt(String s, String s2) {
        return "";
    }
}
