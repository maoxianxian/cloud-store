package playground;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TryMap {
    public static void main(String[] args) {
        Map<String, byte[]> blockMap = new HashMap<>();

        byte[] b = blockMap.get("a");
        assert b==null;

//        ByteString bs = ByteString.copyFrom(b);

//        System.out.println(bs);


        System.out.println(Arrays.toString(blockMap.get(null)));
        assert blockMap.get(null)==null;
        assert !blockMap.containsKey(null);

        blockMap.put(null,new byte[] {'a','b'});
        System.out.println(Arrays.toString(blockMap.get(null)));

        assert Arrays.equals(blockMap.get(null), new byte[] {'a','b'});
        assert blockMap.containsKey(null);
    }
}
