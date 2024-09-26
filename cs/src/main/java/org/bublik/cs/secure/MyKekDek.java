package org.bublik.cs.secure;

import ru.tinkoff.apsv.encryption.core.DefaultEncryptionHelper;
import ru.tinkoff.apsv.encryption.core.KekDekEncryptor;
import ru.tinkoff.apsv.encryption.model.enums.CipherAlgorithmEnum;
import ru.tinkoff.apsv.encryption.model.enums.ImplementationTypeEnum;
import ru.tinkoff.apsv.encryption.model.enums.KeyAlgorithmEnum;
import ru.tinkoff.apsv.encryption.model.model.data.DataDto;
import ru.tinkoff.apsv.encryption.model.model.data.KekDekData;
import ru.tinkoff.apsv.encryption.model.model.encryptedData.KekDekRequestEncryptedData;
import ru.tinkoff.apsv.encryption.model.model.encryptedData.KekDekResponseEncryptedData;
import ru.tinkoff.apsv.encryption.model.model.property.EncryptionConfigProperties;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.util.UUID;

/*
https://tinkoff.ktalk.ru/recordings/JqHcS5uYTxNUV0OoXFHe
https://gitlab.tcsbank.ru/aps/apsv-encryption-manager
https://wiki.tcsbank.ru/pages/viewpage.action?pageId=3596378608
gradle publishToMavenLocal
*/

public class MyKekDek {

    public static void main(String[] args) throws NoSuchAlgorithmException {
        DefaultEncryptionHelper encryptionHelper = new DefaultEncryptionHelper();
        KekDekEncryptor kekDekEncryptor = new KekDekEncryptor(encryptionHelper);
        byte[] secretKey = serializeSecretKey(generateSecretKey(256));
        UUID extId = UUID.randomUUID();
        EncryptionConfigProperties encryptionConfig = new EncryptionConfigProperties(
                extId,
                LocalDateTime.now(),
                12,
                128,
                CipherAlgorithmEnum.AES_GCM_NOPADDING,
                KeyAlgorithmEnum.AES,
                256,
                secretKey,
                ImplementationTypeEnum.KEK_DEK
        );
        KekDekResponseEncryptedData responseEncryptData = kekDekEncryptor.encrypt(
                encryptionConfig,
                KekDekData.builder()
                        .fromDto(new DataDto("SECRET MYKEKDEK", "WORD"))
                        .aad("1")
                        .build());
        KekDekRequestEncryptedData requestEncryptData = KekDekRequestEncryptedData.builder()
                .secretData(responseEncryptData.getSecretData())
                .vector(responseEncryptData.getVector())
                .keyId(responseEncryptData.getKeyId())
                .secretKeyDek(responseEncryptData.getSecretKeyDek())
                .vectorDek(responseEncryptData.getVectorDek())
                .aad("1")
                .build();

        String dec = kekDekEncryptor.decrypt(encryptionConfig, requestEncryptData);
        System.out.println(requestEncryptData.getSecretData());
        System.out.println(dec);
    }

    public static byte[] serializeSecretKey(SecretKey secretKey) {
        return secretKey.getEncoded();
    }

    public static SecretKey generateSecretKey(int keySize) throws NoSuchAlgorithmException {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(keySize, new SecureRandom());
        return keyGen.generateKey();
    }
}
