package myUtils;

import com.google.protobuf.ByteString;
import surfstore.SurfStoreBasic.Block;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BlockUtils {
    public static final int BLOCK_SIZE = 4096;
    public static final String EMPTY_HASH="0";

    // file must exist
    public static List<Block> file2blocks(File file) {

        List<Block> res = new ArrayList<>();
        byte[] bytesArray = null;
        try {
            // might over flow
            bytesArray = new byte[(int) file.length()];
            System.out.println(file.length());
            FileInputStream fis = new FileInputStream(file);
            int n = fis.read(bytesArray); //read file into bytes[]
            if (n <= 0) throw new IOException();
            fis.close();
            System.out.println(bytesArray);
        } catch (FileNotFoundException e) {
            res = new ArrayList<>();
        } catch (IOException e) {
            res = new ArrayList<>();
        }

        int numOfBlocks = (int) Math.ceil((double) file.length() / BLOCK_SIZE);
        System.out.println("numOfBlocks: " + numOfBlocks);

        for (int i = 0; i < numOfBlocks; i++) {
            int st = i * BLOCK_SIZE;
            int ed = Math.min(i * BLOCK_SIZE + BLOCK_SIZE, (int) file.length());
            System.out.println("st: "+st+"\ted: "+ed);
            byte[] bytes = Arrays.copyOfRange(bytesArray, st, ed);
            Block.Builder builder =
                    Block.newBuilder().setData(ByteString.copyFrom(bytes))
                            .setHash(HashUtils.sha256(bytes));

            res.add(builder.build());
        }

        return res;
    }


    static private byte[] concatenate(byte[] a, byte[] b) {
        int aLen = a.length;
        int bLen = b.length;

        @SuppressWarnings("unchecked")
        byte[] c = (byte[]) Array.newInstance(a.getClass().getComponentType(), aLen + bLen);
        System.arraycopy(a, 0, c, 0, aLen);
        System.arraycopy(b, 0, c, aLen, bLen);

        return c;
    }


    public static byte[] concatBlocks(List<Block> blocks) {
        byte[] res = new byte[0];
        for (Block block : blocks) {
            res = concatenate(res, block.getData().toByteArray());
        }
        return res;
    }

    // blockList is composed of hashes
    public static boolean blockListIsDeleted(List<String> hashes){
        if (hashes.isEmpty()){
            return true;
        }
        if (hashes.size()==1 && hashes.get(0).equals(EMPTY_HASH)) {
            return true;
        }
        return false;
    }


    public static boolean existNotDir(File file) {
        return file.exists() && !file.isDirectory();
    }

//    public static class BlockIterable implements Iterable<Block> {
//        private final File file;
//        private InputStream inputStream = null;
//
//        public BlockIterable(File file) throws NoTargetFileException {
//            this.file = file;
//            Files.
//
//            try {
//                this.inputStream = new FileInputStream(file);
//            }
//
//
//
//            try {
//                this.reader = new BufferedReader(new FileReader(file));
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//                throw new NoTargetFileException(this.file.getAbsolutePath());
//            } finally {
//
//            }
//
//            FileReader reader = null;
//            try {
//                reader = new FileReader(file);
//                byte[] chars = new byte[BlockUtils.BLOCK_SIZE];
//                reader.read(chars);
//                reader.close();
//            } catch (IOException e) {
//                e.printStackTrace();
//            } finally {
//                if (reader != null) {
//                    reader.close();
//                }
//            }
//        }
//
//
//        public Iterator<Block> iterator() {
//            return new Iterator<Block>() {
//                @Override
//                public boolean hasNext() {
//                    return false;
//                }
//
//                @Override
//                public Block next() {
//                    return null;
//                }
//            };
//        }
//    }


    public final static class NoActionException extends RuntimeException {
        public static final String MESSAGE = "No such action";
    }

    public final static class NoLocalTargetFileException extends RuntimeException {
        public final String targetFilename;

        public NoLocalTargetFileException(String targetFilename) {
            super();
            this.targetFilename = targetFilename;
        }

    }

    public final static class BlockHashNotFoundException extends RuntimeException {
    }

    public static void main(String[] args) {

    }
}
