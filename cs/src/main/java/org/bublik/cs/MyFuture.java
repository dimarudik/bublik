package org.bublik.cs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyFuture {
    public static void main(String[] args) throws InterruptedException {
        List<String> strings = new ArrayList<>(Arrays.asList(
                "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"));
        ExecutorService service = Executors.newFixedThreadPool(4);
        strings.forEach(s -> CompletableFuture
                .supplyAsync(() -> {
                    try {
                        Thread.sleep((int) (Math.random() * 100));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return Thread.currentThread().getName() + " " + s;
                }, service)
                .thenAccept(System.out::println));
        service.shutdown();
        service.close();
/*
        CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
            String s = Thread.currentThread().getName();
            System.out.println(s);
            return s;
        });
*/
    }
}
