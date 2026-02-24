package top.doe.hive.udf.templates.exec;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

public class MyAvg extends UDAF {

    // 这是UDAF的计算逻辑封装类
    public static class AggEvaluator implements UDAFEvaluator {

        Agg agg;

        public AggEvaluator() {
            super();
            init();
        }

        // 初始化，重点是初始化累加器
        @Override
        public void init() {
            agg = new Agg();
        }


        // 从原始输入数据获得数据后计算
        public boolean iterate(Double value) {
            if (value != null) {
                agg.sum += value;
                agg.cnt += 1;
            }
            return true;
        }


        // 每次局部聚合完成时调用，以输出局部聚合的结果
        // 这个结果会交给全局聚合阶段作为输入
        public Agg terminatePartial() {
            return agg;
        }


        // 全局聚合逻辑
        // 每一个输入数据，就是局部聚合阶段所输出的一个结果数据
        public boolean merge(Agg partialAgg) {
            if (partialAgg != null) {
                agg.sum += partialAgg.sum;
                agg.cnt += partialAgg.cnt;
            }

            return true;
        }


        // 全局聚合完成后，用于输出最终返回结果的逻辑
        public Double terminate() {
            return agg.sum/agg.cnt;
        }


        /**
         *  自定义的累加器类
         */
        public static class Agg {
            double sum;
            int cnt;
        }


    }


}
