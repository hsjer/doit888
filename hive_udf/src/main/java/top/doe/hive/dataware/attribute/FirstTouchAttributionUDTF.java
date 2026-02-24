package top.doe.hive.dataware.attribute;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/10
 * @Desc: 学大数据，上多易教育
 * 首次触点归因策略算法
 **/
public class FirstTouchAttributionUDTF extends GenericUDTF {

    private StandardListObjectInspector arrayOI;
    private static final Logger logger = LoggerFactory.getLogger(FirstTouchAttributionUDTF.class);

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        logger.error("进入了函数的初始化方法------------------------------------------");

        if (args.length != 1 || !(args[0] instanceof StandardListObjectInspector)) {
            throw new UDFArgumentException("ArrayToRowsUDTF only takes an array as parameter");
        }

        // 获取数组的 ObjectInspector
        this.arrayOI = (StandardListObjectInspector) args[0];

        // 设置输出列结构
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                java.util.Arrays.asList("goal_time", "to_attribute_event", "contribution"),
                java.util.Arrays.asList(
                        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
                        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
                        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector
                )
        );
    }


    @Override
    public void process(Object[] args) throws HiveException {
        if (args == null || args[0] == null) {
            return;
        }

        // 从 ObjectInspector 中获取数组内容
        //  ["20000:BAIDU","70000:XHS","95000:GOAL",||  "96000:XHS","97000:XHS","98000:GOAL","99000:XHS"]
        // ["20000:BAIDU","70000:XHS","95000:GOAL"]
        List<?> array = arrayOI.getList(args[0]);

        ArrayList<String> to_attr = new ArrayList<>();
        long goal_time = -1;

        // 按业务目标事件分段处理
        for (int i = 0; i < array.size(); i++) {
            String[] split = array.get(i).toString().split(":");
            long et = Long.parseLong(split[0]);

            String e = split[1];   // 当前遍历到的事件


            if (!e.equals("GOAL")) {
                to_attr.add(e);
                logger.error("遍历到一个非GOAL事件，添加到 list：{} ",to_attr);

            } else {

                logger.error("遍历到一个GOAL事件，准备计算list：{} ",to_attr);

                // 把目标事件发生时间赋值给 goal_time
                goal_time = et;
                if (to_attr.size() == 0) {
                    return;
                } else {
                    // 然后首个事件的共线
                    forward(new Object[]{goal_time, to_attr.get(0),1.0 });
                }

                // 清空  待归因事件列表
                to_attr.clear();

            }


        }

        // forward(new Object[]{word, length})
    }

    @Override
    public void close() throws HiveException {
        // 无需清理
    }
}
