package top.doe.hive.udf.templates.exec;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/8
 * @Desc: 学大数据，上多易教育
 *
 *   按照指定长度，对时间截断取整
 **/
public class TimeFloor extends UDF {

    public String evaluate(Long timestamp, int unit) {
        // 1725524991000
        // 5
        int tmp = unit*60*1000;
        long floorTime = (timestamp / tmp) * tmp;

        return DateFormatUtils.format(floorTime, "yyyy-MM-dd HH:mm:ss");
    }

}
