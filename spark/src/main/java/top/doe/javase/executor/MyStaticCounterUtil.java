package top.doe.javase.executor;

import java.util.HashMap;

public class MyStaticCounterUtil {
    static HashMap<String, Integer> myCounter;

    public static HashMap<String, Integer> getCounter() {

        if (myCounter == null) {
            synchronized ("ok") {
                if(myCounter == null ) {
                    myCounter = new HashMap<>();
                }
            }

        }
        return myCounter;
    }
}
