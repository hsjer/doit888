package top.doe.spark.demos;

import org.apache.spark.api.java.function.Function;

public class LineSplit implements Function<String,String[]> {
    @Override
    public String[] call(String v1) throws Exception {
        return v1.split(" ");
    }
}
