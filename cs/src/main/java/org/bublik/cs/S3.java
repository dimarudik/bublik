package org.bublik.cs;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class S3 {
    public static void main(String[] args) throws URISyntaxException {
        AwsCredentials credentials = AwsBasicCredentials.create(
                "V5f9ndI7xcSGcjIfBe1g",
                "FTVRCqYJGSCVfTl4A7Gkem1RbrFQkD2TbuxxEdGW"
        );
        try (S3Client s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .endpointOverride(new URI("http://localhost:9000"))
                .credentialsProvider(StaticCredentialsProvider.create(credentials))
                .forcePathStyle(true)
                .build()){
            ListBucketsResponse lbResponse = s3.listBuckets();

            for (Bucket bucket : lbResponse.buckets()) {
                System.out.println(bucket.name() + "\t" + bucket.creationDate());
            }

            PutObjectResponse putObjectResponse = putS3Object(s3, "bbb", "aaa", "/tmp/test");

        }
    }

    public static PutObjectResponse putS3Object(S3Client s3, String bucketName, String objectKey, String filename) {
        try {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("x-amz-meta-myVal", "test");
            PutObjectRequest putOb = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .metadata(metadata)
                    .build();

//            PutObjectResponse putObjectResponse = s3.putObject(putOb, RequestBody.fromFile(new File(filename)));
            RequestBody requestBody = RequestBody.fromString("My Content");
            //            System.out.println("Successfully placed " + objectKey + " into bucket " + bucketName);
            return s3.putObject(putOb, requestBody);
//            return putObjectResponse;

        } catch (S3Exception e) {
//            System.err.println(e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        return null;
    }
}
