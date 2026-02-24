package top.doe.spark.demos;

import java.math.BigDecimal;
import java.util.ArrayList;

public class _Test {
    public static void main(String[] args) {

        BigDecimal d1 = BigDecimal.valueOf(10.0);
        BigDecimal d2 = BigDecimal.valueOf(20.0);


        BigDecimal added = d1.add(d2);

        System.out.println(d1);
        System.out.println(d2);
        System.out.println(added);

    }
}
