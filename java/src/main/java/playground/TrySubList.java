package playground;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TrySubList {
    public static void printList(List list){
        System.out.println(Arrays.toString(list.toArray()));
    }

    public static void main(String[] args){
        List<Integer> a = new ArrayList<>();

        a.add(0);
        a.add(1);

        printList(a);

        List<Integer> b = a.subList(0,1);
        printList(b);

        b = a.subList(0,0);
        printList(b);

        b = a.subList(0,-1);
        printList(b);

    }
}
