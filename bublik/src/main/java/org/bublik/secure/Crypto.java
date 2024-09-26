package org.bublik.secure;

public interface Crypto<E, D, K, X, Y> {
    E encrypt(K k, X x);
    D decrypt(K k, Y y);
}
