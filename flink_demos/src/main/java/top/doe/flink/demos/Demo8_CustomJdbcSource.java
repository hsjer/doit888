package top.doe.flink.demos;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/10/11
 * @Desc: 学大数据，上多易教育
 *   自定义SourceFunction示例
 **/
public class Demo8_CustomJdbcSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Person> stream = env.addSource(new SourceFunction<Person>()
        {
            Connection connection;
            Statement stmt;
            ResultSet resultSet;

            // 工作逻辑
            @Override
            public void run(SourceContext<Person> ctx) throws Exception {

                connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/doit50", "root", "ABC123.abc123");
                stmt = connection.createStatement();
                resultSet = stmt.executeQuery("select id,name,gender,salary from t_person");

                while (resultSet.next()) {
                    int id = resultSet.getInt("id");
                    String name = resultSet.getString("name");
                    String gender = resultSet.getString("gender");
                    double salary = resultSet.getDouble("salary");

                    Person person = new Person(id, name, gender, salary);

                    ctx.collect(person);

                    Thread.sleep(1000);
                }
            }

            // 任务取消或结束的收尾逻辑
            @Override
            public void cancel() {

                try {
                    resultSet.close();
                    stmt.close();
                    connection.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });


        stream.print();


        env.execute();

    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Person implements Serializable{
        private int id;
        private String name;
        private String gender;
        private double salary;
    }

}
