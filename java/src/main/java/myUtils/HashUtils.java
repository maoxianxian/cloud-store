package myUtils;

import java.awt.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class HashUtils {
    public static String sha256(byte[] bytes) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
//        byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
        byte[] hash = digest.digest(bytes);
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }

    public static String sha256_str(String text) {
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
//        byte[] hash = digest.digest(bytes);
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }

    public static void main(String[] args) {
//        if (args.length != 1) {
//            System.err.println("Usage: ShaTest <string>");
//            System.exit(1);
//        }

//        String text = args[0];
        String text = "abcdefg";
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        System.out.println("Input: " + text);
        String encoded;
        encoded = sha256_str(text);
        System.out.println("Output: " + encoded);

        System.out.println("-------------");
        System.out.println("bytes");
        System.out.println("Input: " + new String(bytes));
        encoded = sha256(bytes);
        System.out.println("Output: " + encoded);
    }
}
