package playground;

import myUtils.BlockUtils;
import surfstore.SurfStoreBasic.*;

import java.util.List;


public class TryFileInfoBlockList {
    public static void main(String[] args){
        // not setBlocklists
//        FileInfo fileInfo = FileInfo.newBuilder().setBlocklist(0,"0").build();
        FileInfo fileInfo = FileInfo.newBuilder().addBlocklist(BlockUtils.EMPTY_HASH).build();

        List<String> blocks = fileInfo.getBlocklistList();

        System.out.println(blocks.size());
    }
}
