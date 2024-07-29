package org.bublik.cs;

import io.minio.*;
import io.minio.errors.MinioException;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class FileUploader {
    public static void main(String[] args)
            throws IOException, NoSuchAlgorithmException, InvalidKeyException {
        try {
            // Create a minioClient with the MinIO server playground, its access key and secret key.
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint("http://localhost:9000")
                            .credentials("V5f9ndI7xcSGcjIfBe1g", "FTVRCqYJGSCVfTl4A7Gkem1RbrFQkD2TbuxxEdGW")
                            .build();

            // Make 'asiatrip' bucket if not exist.
            boolean found =
                    minioClient.bucketExists(BucketExistsArgs.builder().bucket("asiatrip").build());
            if (!found) {
                // Make a new bucket called 'asiatrip'.
                minioClient.makeBucket(MakeBucketArgs.builder().bucket("asiatrip").build());
            } else {
                System.out.println("Bucket 'asiatrip' already exists.");
            }

            // Upload '/home/user/Photos/asiaphotos.zip' as object name 'asiaphotos-2015.zip' to bucket
            // 'asiatrip'.
            ObjectWriteResponse objectWriteResponse = minioClient.uploadObject(
                    UploadObjectArgs.builder()
                            .bucket("asiatrip")
                            .object("test")
                            .filename("/tmp/test")
                            .build());
            System.out.println(objectWriteResponse.etag());
            System.out.println(
                    "'/tmp/test' is successfully uploaded as "
                            + "object 'test' to bucket 'asiatrip'.");
            minioClient.close();
        } catch (MinioException e) {
            System.out.println("Error occurred: " + e);
            System.out.println("HTTP trace: " + e.httpTrace());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
