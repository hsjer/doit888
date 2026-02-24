package top.doe.hive.udf.templates.old;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class UDAFExample_2 extends UDAF {

    // 定义一个内部类来实现 UDAFEvaluator 接口
    public static class SumUDAFEvaluator implements UDAFEvaluator {
        // 用来存储聚合结果的变量
        private double result;

        // 构造函数，用来初始化聚合函数
        public SumUDAFEvaluator() {
            super();
            init();
        }

        // 初始化聚合计算的状态
        public void init() {
            result = 0.0;
        }

        // 对原始输入数据进行计算
        public boolean iterate(Double value) {
            if (value != null) {
                result += value;
            }
            return true;
        }

        // 返回可能的局部聚合结果
        public Double terminatePartial() {
            return result;
        }

        // 合并各个 聚合聚合结果
        public boolean merge(Double partialResult) {
            if (partialResult != null) {
                result += partialResult;
            }
            return true;
        }

        // 返回 最终的输出结果
        public Double terminate() {
            return result;
        }
    }



}
