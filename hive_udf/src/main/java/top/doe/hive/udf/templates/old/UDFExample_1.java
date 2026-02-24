package top.doe.hive.udf.templates.old;

import org.apache.hadoop.hive.ql.exec.UDF;

public class UDFExample_1 extends UDF {
    public String evaluate(String str) {
        return str.toUpperCase();
    }
}
