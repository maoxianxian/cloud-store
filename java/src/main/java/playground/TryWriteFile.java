package playground;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.logging.Logger;

public class TryWriteFile {
    static Logger logger = Logger.getLogger(TryWriteFile.class.getName());
    public static void main(String[] args) {
        try {
            byte[] bytes = "abcdefghijklmnopqrstuvwxyz".getBytes("UTF-8");


            String relativePath = "java/src/main/java/playgroundData/1111.txt";
            File file = new File(relativePath);
            System.out.println(file.getAbsolutePath());
//            System.out.println(System.getProperty("user.dir")+"/"+relativePath);
//            assert file.getAbsolutePath().equals(System.getProperty("user.dir")+'/'+relativePath);
//            assert file.exists() && !file.isDirectory();

            String filename = file.getAbsolutePath();

            FileOutputStream stream = new FileOutputStream(filename);
            try {
                stream.write(bytes);
            } finally {
                stream.close();
            }
        }
        catch (FileNotFoundException e) {
            logger.info("File not found");
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
