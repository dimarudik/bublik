package org.chekist;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.bublik.secure.EncryptedEntity;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static org.chekist.Utils.*;


public class ChekistEntity extends EncryptedEntity {
    private final byte[] encryptedDEKByKEK;
    private final byte[] encryptedTextBytes;
    private final byte[] aad;
    private final byte[] kekVector;
    private final byte[] dekVector;

    public ChekistEntity(ChekistConfig config, ChekistData data) throws NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, NoSuchPaddingException, InvalidKeyException,
            IllegalBlockSizeException, BadPaddingException {
        this.dekVector = generateVector(config.getVectorSize());
        this.kekVector = generateVector(config.getVectorSize());
        byte[] DEK = serializeSecretKey(generateSecretKey(config.getKeySize(), config.getAlgorithm())); ; // генерится рандомно
        byte[] KEK = decode(data.getKeyEncryptionKey());
        Cipher dekCipher =
                initCipher(DEK, dekVector, Cipher.ENCRYPT_MODE, config.getAlgorithm(), config.getTransformation(), config.gettLen());
        Cipher kekCipher =
                initCipher(KEK, kekVector, Cipher.ENCRYPT_MODE, config.getAlgorithm(), config.getTransformation(), config.gettLen());

        this.aad = data.getAad().getBytes();
        this.encryptedDEKByKEK = kekCipher.doFinal(DEK);
        dekCipher.updateAAD(this.aad);

        this.encryptedTextBytes = dekCipher.doFinal(data.getPlainText().getBytes());
    }

    public byte[] getEncryptedDEKByKEK() {
        return encryptedDEKByKEK;
    }

    public byte[] getEncryptedTextBytes() {
        return encryptedTextBytes;
    }

    public byte[] getAad() {
        return aad;
    }

    public byte[] getKekVector() {
        return kekVector;
    }

    public byte[] getDekVector() {
        return dekVector;
    }

    @Override
    public String obtainEncryptedData() {
        return encode(getEncryptedTextBytes());
    }

    @Override
    public String obtainEncryptedMetaData() {
        try {
            ObjectWriter mapperJSON = new ObjectMapper().writer().withDefaultPrettyPrinter();
            return mapperJSON.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
