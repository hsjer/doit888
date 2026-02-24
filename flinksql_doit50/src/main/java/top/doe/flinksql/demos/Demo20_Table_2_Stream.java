package top.doe.flinksql.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class Demo20_Table_2_Stream {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("file:///d:/ckpt");
        env.setParallelism(1);

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 创建源表
        tenv.executeSql(
                "create temporary table tpc_1 (  \n" +
                "    id int,           \n" +
                "    name string,      \n" +
                "    gender string,    \n" +
                "    score double      \n" +
                ") with (              \n" +
                "    'connector' = 'kafka',            \n" +
                "    'topic' = 'topic-1',              \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'g001',       \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")");



        tenv.executeSql(
                "create temporary view view_tmp as " +
                "select id,name,gender,score from tpc_1 " +
                "where score>=60");

        // 把sql名称的表，转成tableApi中的表对象
        Table table = tenv.from("view_tmp");

        // 方式一、把表对象，转成流,不指定目标类型，则转成了Row类型
        DataStream<Row> rowDataStream = tenv.toDataStream(table);
        rowDataStream.map(new MapFunction<Row, String>() {
            @Override
            public String map(Row row) throws Exception {
                int id = row.getFieldAs("id");
                String name = row.getFieldAs("name");
                return null;
            }
        });


        // 方式二、把表对象，转成流,显式指定目标类型，则转成了目标类型的流
        DataStream<Student> studentDataStream = tenv.toDataStream(table, Student.class);
        studentDataStream.map(new MapFunction<Student, String>() {
            @Override
            public String map(Student student) throws Exception {
                Integer id = student.id;
                return null;
            }
        });


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

}
