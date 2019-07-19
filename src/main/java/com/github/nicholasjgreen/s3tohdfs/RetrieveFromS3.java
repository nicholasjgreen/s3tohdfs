package com.github.nicholasjgreen.s3tohdfs;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SDKGlobalConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.gson.Gson;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.io.IOUtils;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;


public class RetrieveFromS3 {

    private void run(String[] args) throws IOException {
        System.out.println("RetrieveFromS3()");

        System.out.println("Setting up");
        String clientRegion = "";
        String bucketName = "";
        //String key = "airline_reviews/airline.csv.bz2.enc";

        System.out.println("AWS Api access");
        //System.setProperty(SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY, api_key);
        //System.setProperty(SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY, api_secret);
        AWSCredentialsProvider provider = new ProfileCredentialsProvider();//new SystemPropertiesCredentialsProvider();

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            System.out.println("Building client");

            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(provider)
                    .build();


            String keyFilePath = "airline_reviews/airline.csv.bz2.keys";
            String cipherFilePath = "airline_reviews/airline.csv.bz2.enc";

            System.out.println("Retrieving decryption keys");
            // Retrieve the keys file
            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, keyFilePath));
            String encryptionKeyJson = IOUtils.toString(fullObject.getObjectContent());
            Gson g = new Gson();
            EncryptionKeyValues encryptionKeyValues = g.fromJson(encryptionKeyJson, EncryptionKeyValues.class);

            // Get an object and print its contents.
            /*System.out.println("Downloading an object");
            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            System.out.println("Content: ");
            displayTextInputStream(fullObject.getObjectContent());*/

            // Get a range of bytes from an object and print the bytes.
            /*GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(0,128);
            objectPortion = s3Client.getObject(rangeObjectRequest);*/
            //System.out.println("Printing bytes retrieved.");
            //displayTextInputStream(objectPortion.getObjectContent());

            System.out.println("Parsing keys");
            Base64 base64 = new Base64();
            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            SecretKeySpec secretKeySpec = new SecretKeySpec(base64.decode(encryptionKeyValues.key), "AES");
            IvParameterSpec ivParameterSpec = new IvParameterSpec(base64.decode(encryptionKeyValues.iv));
            System.out.println("Key size = " + secretKeySpec.getEncoded().length);
            System.out.println("Intialising cipher");
            cipher.init(Cipher.DECRYPT_MODE, secretKeySpec, ivParameterSpec);

            System.out.println("Retrieving encrypted data");
            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, cipherFilePath));
            OutputStream out = Files.newOutputStream(Paths.get("./decrypted_decompressed.csv"));
            BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(new CipherInputStream(fullObject.getObjectContent(), cipher));
            final byte[] buffer = new byte[1024 * 1024 * 100];
            int n;
            while (-1 != (n = bzIn.read(buffer))) {
                out.write(buffer, 0, n);
            }

            BufferedReader reader;
            try {
                reader = new BufferedReader(new FileReader(
                        "./decrypted_decompressed.csv"));
                String line = reader.readLine();
                while (line != null) {
                    System.out.println(line);
                    // read next line
                    line = reader.readLine();
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            System.out.println("Done!");
            /*try (InputStream is = new CipherInputStream(fullObject.getObjectContent(), cipher)) {
                Files.copy(is, Paths.get("./decrypted.csv"));
            }*/


            // Get an entire object, overriding the specified response headers, and print the object's content.
            /*ResponseHeaderOverrides headerOverrides = new ResponseHeaderOverrides()
                    .withCacheControl("No-cache")
                    .withContentDisposition("attachment; filename=example.txt");
            GetObjectRequest getObjectRequestHeaderOverride = new GetObjectRequest(bucketName, key)
                    .withResponseHeaders(headerOverrides);
            headerOverrideObject = s3Client.getObject(getObjectRequestHeaderOverride);
            displayTextInputStream(headerOverrideObject.getObjectContent());*/
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (InvalidAlgorithmParameterException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if(fullObject != null) {
                fullObject.close();
            }
            if(objectPortion != null) {
                objectPortion.close();
            }
            if(headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Running RetrieveFromS3!");
        new RetrieveFromS3().run(args);
        System.out.println("Done!");
    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read the text input stream one line at a time and display each line.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = null;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
        System.out.println();
    }

}
