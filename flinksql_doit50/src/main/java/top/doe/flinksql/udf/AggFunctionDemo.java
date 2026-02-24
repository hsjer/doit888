package top.doe.flinksql.udf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class AggFunctionDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

/*

        // 造数据流
        DataStreamSource<Order> stream = env.fromElements(
                new Order(1, 100, "江西"),
                new Order(2, 200, "江西"),
                new Order(3, 100, "山东"),
                new Order(4, 200, "山东")

        );
        // 流转表
        tenv.createTemporaryView("t_od",stream);

        // 注册函数
        tenv.createTemporaryFunction("my_avg",MyAvgFunction.class);

        // sql
        tenv.executeSql("select province,my_avg(amt)  from t_od group by province").print();
*/


        // 建表
        tenv.executeSql(
                " create table order_cdc   (                   "+
                        "     oid int primary key not enforced,"+
                        "     amt int,                         "+
                        "     province string                  "+
                        " ) with (                             "+
                        "     'connector' = 'mysql-cdc',       "+
                        "     'hostname' = 'doitedu01',        "+
                        "     'port' = '3306',                 "+
                        "     'username' = 'root',             "+
                        "     'password' = 'ABC123.abc123',    "+
                        "     'database-name' = 'doit50',      "+
                        "     'table-name' = 'agg_test'         "+
                        " )                                    "
        );

        // 注册函数
        tenv.createTemporaryFunction("my_avg",MyAvgFunction.class);

        // sql
        tenv.executeSql("select province,my_avg(amt)  from order_cdc group by province").print();

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        private int oid;
        private Integer amt;
        private String province;
    }


    // 定义一个累加器
    public static class AvgAcc {
        public int count;
        public int sum;
    }

    // 定义聚合函数 : 泛型1：最终返回结果类型， 泛型2：累加器类型
    public static class MyAvgFunction extends AggregateFunction<Double,AvgAcc>{


        // 取聚合的最终结果
        @Override
        public Double getValue(AvgAcc accumulator) {
            if(accumulator.count == 0){
                return null;
            }else{
                return (double)accumulator.sum/accumulator.count;
            }

        }

        // 创建累加器对象
        @Override
        public AvgAcc createAccumulator() {
            return new AvgAcc();
        }


        // 累加数据 ： 处理 +I/+U
        public void accumulate(AvgAcc acc, Integer amt) {
            acc.count++;
            acc.sum += amt;

        }

        // 回撤: 处理 -U/-D
        public void retract(AvgAcc acc, Integer amt) {
            acc.count--;
            acc.sum -= amt;
        }


        // 合并多个累加器的数据
        public void merge(AvgAcc acc, Iterable<AvgAcc> it) {
            for (AvgAcc other : it) {
                acc.count += other.count;
                acc.sum += other.sum;
            }
        }


        // 累加器重置初始值
        public void resetAccumulator(AvgAcc acc) {
            acc.count = 0;
            acc.sum = 0;
        }

    }

}
