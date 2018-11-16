package playground;

import com.google.protobuf.ByteString;
import surfstore.SurfStoreBasic.*;

import java.util.ArrayList;

public class TryProtobufNull {
    public static void main(String[] args) {
        Block b = Block.newBuilder().setHash("aaa").build();

        assert b.getHash().equals("aaa") : "AAA";
        assert b.getData() == ByteString.EMPTY : "BBB";

        FileInfo fi = FileInfo.newBuilder().build();
        assert fi.getVersion()==0;
        assert fi.getBlocklistList().equals(new ArrayList<String>());
//        assert fi.getBlocklistList()==null;
        assert fi.getFilename().equals("");

//        assert fi.getFilename()==null;
//        assert fi.getFilename().equals(null);
    }
}
