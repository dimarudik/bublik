package org.bublik.cs;

import software.amazon.awssdk.services.s3.endpoints.internal.Value;

import java.util.UUID;

public class MyUUID {
    public static void main(String[] args) {
//        9d030f4f-5df1-4b16-a7c5-64b5e50343c6
//        37de91d8-e948-4f43-940b-68f4b7d538e0
        String s = "9d030f4f-5df1-4b16-a7c5-64b5e50343c6";
        UUID.fromString(s);
//        UUID uuid = (UUID) o;
    }
}
