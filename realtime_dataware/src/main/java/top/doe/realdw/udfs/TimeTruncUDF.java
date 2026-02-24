package top.doe.realdw.udfs;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.flink.table.functions.ScalarFunction;

import java.io.Serializable;

public class TimeTruncUDF extends ScalarFunction implements Serializable {
    public String eval(Long time,int unit) {

        int trunc_unit = unit*60*1000;

        long trunked = (time/trunc_unit) * trunc_unit;

        return DateFormatUtils.format(trunked,"yyyy-MM-dd HH:mm:ss");
    }
}
