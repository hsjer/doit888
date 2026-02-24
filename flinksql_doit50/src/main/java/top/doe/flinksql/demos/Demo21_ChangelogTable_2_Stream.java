package top.doe.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class Demo21_ChangelogTable_2_Stream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建源表
        tenv.executeSql(
                " create table order_cdc(                      "+
                        "     oid int primary key not enforced,"+
                        "     uid int,                         "+
                        "     amt decimal(10,2),               "+
                        "     create_time timestamp(3),        "+
                        "     status int                       "+
                        " ) with (                             "+
                        "     'connector' = 'mysql-cdc',       "+
                        "     'hostname' = 'doitedu01',        "+
                        "     'port' = '3306',                 "+
                        "     'username' = 'root',             "+
                        "     'password' = 'ABC123.abc123',    "+
                        "     'database-name' = 'doit50',      "+
                        "     'table-name' = 't_order'         "+
                        " )                                    "
        );


        // 先把sql名称表，转成TableApi的表对象
        Table table = tenv.from("order_cdc");

        // 把表对象，转成dataStream
        DataStream<Row> rowDataStream = tenv.toChangelogStream(table);

        // 如何获取row的类型： +I/-U/+U/-D
        SingleOutputStreamOperator<String> resultStream = rowDataStream.process(new ProcessFunction<Row, String>() {
            @Override
            public void processElement(Row row, ProcessFunction<Row, String>.Context context, Collector<String> collector) throws Exception {

                RowKind kind = row.getKind();
                /*if(kind.shortString().equals("+I")){
                }

                if(kind.toByteValue() == 0 ){
                }*/

                if (kind == RowKind.INSERT) {
                    int uid = row.getFieldAs("uid");
                    int oid = row.getFieldAs("oid");
                    collector.collect(uid + "," + oid);
                } else if (kind == RowKind.UPDATE_BEFORE) {

                    int uid = row.getFieldAs("uid");
                    int oid = row.getFieldAs("oid");
                    collector.collect(uid + "," + oid);

                } else if (kind == RowKind.UPDATE_AFTER) {

                    int uid = row.getFieldAs("uid");
                    int oid = row.getFieldAs("oid");
                    collector.collect(uid + "," + oid);

                } else if (kind == RowKind.DELETE) {
                    throw new BuTinghuaException("不支持的rowKind");
                }


            }
        });


        resultStream.print();

        env.execute();

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student implements Serializable {
        private Integer id;
        private String name;
        private String gender;
        private Double score;
    }


    public static class BuTinghuaException extends RuntimeException{

        public BuTinghuaException(String msg){
            super(msg);
        }


    }


}
