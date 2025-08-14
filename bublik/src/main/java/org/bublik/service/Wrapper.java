package org.bublik.service;

public interface Wrapper {
    <T> T unwrap(Class<T> iface);
    boolean isWrapperFor(Class<?> iface);
}
