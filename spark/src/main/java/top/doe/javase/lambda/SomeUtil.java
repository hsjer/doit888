package top.doe.javase.lambda;

import java.util.List;

public class SomeUtil {

    public String lenX(SomeInterface si, List<Integer> data){
        String res = si.func(data);
        return res.toUpperCase();
    }

}
