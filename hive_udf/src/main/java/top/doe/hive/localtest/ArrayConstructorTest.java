package top.doe.hive.localtest;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ArrayConstructorTest {
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        char[] chars = new char[8];

        Class<? extends char[]> aClass = chars.getClass();  // [C

        char[] o = (char[]) Array.newInstance(char.class, 20);
        o[0] = 'a';
        o[1] = 'b';

        System.out.println(o);

    }
}
