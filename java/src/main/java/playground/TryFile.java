package playground;

import myUtils.BlockUtils;
import surfstore.SurfStoreBasic;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TryFile {
    public static void main(String[] args) throws Throwable {
        String relativePath = "java/src/main/java/playgroundData/1.3.txt";
        File file = new File(relativePath);
        System.out.println(file.getAbsolutePath());
        System.out.println(System.getProperty("user.dir")+"/"+relativePath);
        assert file.getAbsolutePath().equals(System.getProperty("user.dir")+'/'+relativePath);
        assert file.exists() && !file.isDirectory();

        List<SurfStoreBasic.Block> res = new ArrayList<>();
        byte[] bytesArray = null;
        try {
            // might over flow
//            bytesArray = new byte[(int)file.length()];
            bytesArray = new byte[100];
            FileInputStream fis = new FileInputStream(file);
            int n = fis.read(bytesArray); //read file into bytes[]
            if (n <= 0) throw new IOException();
            bytesArray = Arrays.copyOfRange(bytesArray, 0, n);
            fis.close();
        } catch (FileNotFoundException e) {
            res = new ArrayList<>();
        } catch (IOException e) {
            res = new ArrayList<>();
        }

        String str = new String(bytesArray);
        System.out.println(str);
        System.out.println(str.length());
        System.out.println(str.getBytes("UTF-8").length);

        System.out.println("------------------");

        List<SurfStoreBasic.Block> blocks = BlockUtils.file2blocks(file);
        for(SurfStoreBasic.Block block : blocks){
            System.out.println("Hash: "+block.getHash());
            System.out.println("Data: "+block.getData().toByteArray().length);
        }

        System.out.println(file.getName());


        file = new File("/Users");
        assert file.isDirectory();
        assert file.getName().equals("Users");

        file = new File("/Users/");
        assert file.isDirectory();
        assert file.getName().equals("Users");

        file = new File("/");
        assert file.isDirectory();
        assert file.getName().equals("");

        file = new File("/Users/abc");
        assert !file.isDirectory();
        assert file.getName().equals("abc");

        file = new File("/Users/rongjinqiao/Codes/18sp/CSE291C/p2-rjqiao");
        assert file.isDirectory();
        assert file.getAbsolutePath().equals("/Users/rongjinqiao/Codes/18sp/CSE291C/p2-rjqiao");


        file = new File("/Users/rongjinqiao/Codes/18sp/CSE291C/p2-rjqiao/");
        assert file.isDirectory();
        assert file.getAbsolutePath().equals("/Users/rongjinqiao/Codes/18sp/CSE291C/p2-rjqiao/");


    }
}
