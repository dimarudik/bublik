package org.bublik.cs;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MyFuture {
    public static void main(String[] args) {
        List<String> strings = new ArrayList<>(Arrays.asList(
                "a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q","r","s","t","u","v","w","x","y","z"));
        ExecutorService service = Executors.newFixedThreadPool(4);
        strings.forEach(s ->
                CompletableFuture
                        .supplyAsync(() -> getThreadName(s), service)
                        .thenApply(MyFuture::getThreadName)
                        .thenApply(MyFuture::getThreadName)
                        .thenAccept(System.out::println));
        service.shutdown();
        service.close();
    }

    public static String getThreadName(String string) {
        return Thread.currentThread().getName() + " : " + string;
    }
}
