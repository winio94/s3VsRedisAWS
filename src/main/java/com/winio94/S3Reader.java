package com.winio94;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class S3Reader implements RequestStreamHandler {
    private static final List<String> s3Paths = new ArrayList<>();
    private static final Logger logger = LogManager.getLogger(S3Reader.class);
    private static final String S3_BUCKET = "s3://cache-testing-bucket/";
    private final S3Client s3Client;

    public S3Reader() {
        this.s3Client = S3Client.create();
        s3Paths.add("openid-configuration.json");
        s3Paths.add("openid-configuration2.json");
        s3Paths.add("certs1/95508631_www.example.com.cert");
        s3Paths.add("certs1/95508631_www.example.com.key");
        s3Paths.add("certs2/88269526_selfsignedcertificate.com.cert");
        s3Paths.add("certs2/88269526_selfsignedcertificate.com.key");
    }

    @Override
    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {
        logger.info("Lambda START");

        try {
            for (String path : s3Paths) {
                readFileFromS3(path);
            }
        } catch (IOException e) {
            logger.info(String.format("Error during fetching object from S3, %s", e));
        }
    }

    private void readFileFromS3(String filePath) throws IOException {
        String path = S3_BUCKET + filePath;
        AmazonS3URI uri = new AmazonS3URI(path);
        String bucketName = uri.getBucket();
        String key = uri.getKey();
        logger.info(String.format("Downloading %s", filePath));
        String fullObject = IOUtils.toString(s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(), ResponseTransformer.toInputStream()));
        logger.info(String.format("Successfully downloaded %s from S3", filePath));
        logger.info(String.format("Content: %s", fullObject));
    }

}