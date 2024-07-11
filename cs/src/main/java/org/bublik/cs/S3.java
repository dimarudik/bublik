package org.bublik.cs;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectResult;

public class S3 {
    public static void main(String[] args)  {
        // https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/examples-s3-objects.html#upload-object
        AWSCredentials credentials = new BasicAWSCredentials(
                "1wgTNWFwWXLPp4yQctcV",
                "SB2pLWCoWOSGWiMzLh8mWzbpfissphUuS6GQD2Bo"
        );
        AwsClientBuilder.EndpointConfiguration endpointConfiguration =
                new AwsClientBuilder.EndpointConfiguration("http://localhost:9000", Regions.DEFAULT_REGION.getName());
        AmazonS3 amazonS3 = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withEndpointConfiguration(endpointConfiguration)
                .build();
        amazonS3.listBuckets().forEach(b -> System.out.println(b.getName() + ":" + b.getCreationDate()));
        // https://docs.ceph.com/en/latest/radosgw/s3/java/
/*
        AwsCredentials credentials = AwsBasicCredentials.create(
                "1wgTNWFwWXLPp4yQctcV",
                "SB2pLWCoWOSGWiMzLh8mWzbpfissphUuS6GQD2Bo"
        );
        try (S3Client client = S3Client.builder()
                    .endpointOverride(new URI("http://localhost:9000"))
                    .credentialsProvider(StaticCredentialsProvider.create(credentials))
                    .serviceConfiguration(S3Configuration.Builder::pathStyleAccessEnabled)
                    .region(Region.US_EAST_1)
                    .build()) {
            ListBucketsResponse lbResponse = client.listBuckets();
            for (Bucket bucket : lbResponse.buckets()) {
                System.out.println(bucket.name() + "\t" + bucket.creationDate());
            }
            ByteBuffer input = ByteBuffer.wrap("Hello World!".getBytes());
            client.putObject(
                    req -> req.bucket("ddd").key("hello.txt"),
                    RequestBody.fromByteBuffer(input)
            );
        }
*/
    }
}
