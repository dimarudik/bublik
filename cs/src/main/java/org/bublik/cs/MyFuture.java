package org.bublik.cs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyFuture {
    public static void main(String[] args) {
        ExecutorService service = Executors.newFixedThreadPool(4);
        List<String> chunks = new ArrayList<>(Arrays.asList(
                "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"));
        chunks.forEach(chunk ->
                CompletableFuture
                        .supplyAsync(() -> chunk + Thread.currentThread().getName(), service)
                        .thenAccept(s -> System.out.println(s + "; thenAccept: " + Thread.currentThread().getName())));
        service.shutdown();
        service.close();
    }
}
