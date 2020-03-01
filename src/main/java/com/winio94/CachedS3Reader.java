package com.winio94;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.util.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class CachedS3Reader implements RequestStreamHandler {
    private static final List<String> s3Paths = new ArrayList<>();
    private static final Logger logger = LogManager.getLogger(CachedS3Reader.class);
    private static final String S3_BUCKET = "s3://cache-testing-bucket/";
    private final S3Client s3Client;
    private final RedissonClient redisson;

    public CachedS3Reader() {
        this.s3Client = S3Client.create();
        Config config = new Config();
        config.useSingleServer().setAddress("redis://s3-redis-cache.hzvgwm.ng.0001.use1.cache.amazonaws.com:6379");
        this.redisson = Redisson.create(config);

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
                boolean found = readFileFromRedis(path);
                if(!found) {
                    logger.warn(String.format("Cache miss for %s", path));
                    String file = readFileFromS3(path);
                    saveInCache(path, file);
                } else {
                    logger.info(String.format("Cache hit for %s", path));
                }
            }
        } catch (IOException e) {
            logger.info(String.format("Error during fetching object from S3, %s", e));
        }
    }

    private void saveInCache(String path, String content) {
        logger.info(String.format("Storing new object in CACHE %s", path));
        RBucket<String> bucket = redisson.getBucket(path);
        bucket.set(content);
    }

    private boolean readFileFromRedis(String filePath) {
        RBucket<String> bucket = redisson.getBucket(filePath);
        if(!bucket.isExists()) {
            return false;
        } else {
            String objectValue = bucket.get();
            logger.info(String.format("Successfully downloaded %s from CACHE", filePath));
            logger.info(String.format("Content: %s", objectValue));
            return true;
        }
    }

    private String readFileFromS3(String filePath) throws IOException {
        String path = S3_BUCKET + filePath;
        AmazonS3URI uri = new AmazonS3URI(path);
        String bucketName = uri.getBucket();
        String key = uri.getKey();
        logger.info(String.format("Downloading %s", filePath));
        String fullObject = IOUtils.toString(s3Client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build(), ResponseTransformer.toInputStream()));
        logger.info(String.format("Successfully downloaded %s from S3", filePath));
        logger.info(String.format("Content: %s", fullObject));
        return fullObject;
    }

}