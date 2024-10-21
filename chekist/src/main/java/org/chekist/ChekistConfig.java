package org.chekist;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.bublik.secure.SecureConfig;

import java.io.IOException;
import java.nio.file.Paths;

public class ChekistConfig extends SecureConfig {
    private int keySize;
    private int vectorSize;
    private String algorithm;
    private String transformation;
    private int tLen;

    public ChekistConfig(){}

    public ChekistConfig(String fileName) {
        try {
            ObjectMapper mapperJSON = new ObjectMapper();
            ChekistConfig config = mapperJSON.readValue(Paths.get(fileName).toFile(), ChekistConfig.class);
            this.keySize = config.keySize;
            this.vectorSize = config.vectorSize;
            this.algorithm = config.algorithm;
            this.transformation = config.transformation;
            this.tLen = config.tLen;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public int getKeySize() {
        return keySize;
    }

    public int getVectorSize() {
        return vectorSize;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public String getTransformation() {
        return transformation;
    }

    public int gettLen() {
        return tLen;
    }
}
