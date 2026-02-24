package top.doe.javase.lambda;

import java.util.Arrays;
import java.util.List;

public class UtilTest {
    public static void main(String[] args) {

        SomeUtil someUtil = new SomeUtil();

        List<Integer> data = Arrays.asList(1, 2, 3);

        someUtil.lenX(new SomeInterface() {
                          @Override
                          public String func(List<Integer> lst) {
                              return "aaa" + lst.size();
                          }
                      }, data
        );

        someUtil.lenX((lst) -> lst.size() + "bbb",  data);


    }
}
