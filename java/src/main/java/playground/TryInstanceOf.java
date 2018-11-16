package playground;

public class TryInstanceOf {
    public static int f(){
        return 10;
    }

    public static void main(String[] args){
        Object a = (int) f();
        assert a instanceof Integer;
//        assert a instanceof Double;
    }
}
