package top.doe.flinksql.udf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.util.Date;

public class ScalarDemo {


    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 造数据流
        DataStreamSource<Order> stream = env.fromElements(
                new Order(1, 20.2, "2024-10-26 10:17:38.200"),
                new Order(2, 30.2, "2024-10-26 10:19:31.300"),
                new Order(3, 40.2, "2024-10-26 10:21:39.400"),
                new Order(4, 50.2, "2024-10-26 10:25:42.500")

        );


        // 流转表
        tenv.createTemporaryView("t_od",stream);

        // 注册函数
        tenv.createTemporaryFunction("time_trunc", TimeTruncFunction.class);

        // 写sql
        tenv.executeSql("select oid,amt,create_time,time_trunc(create_time,'yyyy-MM-dd HH:mm:ss.SSS',10) as minute_10 from t_od")
                .print();


    }


    public static class TimeTruncFunction extends ScalarFunction{

        public String eval(String timeStr,String dateFormat,Integer trunc_unit){
            // timeStr : 2024-10-26 10:17:32.200
            // trunc_unit : 5

            long unit = trunc_unit*60*1000;

            try {
                Date date = DateUtils.parseDate(timeStr, dateFormat);
                long time = date.getTime();
                long trunked = (time/unit)*unit;

                return DateFormatUtils.format(trunked,dateFormat);

            } catch (ParseException e) {
               return null;
            }
        }

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Order{
        private int oid;
        private double amt;
        private String create_time;
    }

}
