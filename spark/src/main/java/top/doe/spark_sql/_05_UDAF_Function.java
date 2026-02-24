package top.doe.spark_sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class _05_UDAF_Function {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("niube")
                .getOrCreate();

        //-------------JSON------------------------------------------------------
        Dataset<Row> ds3 = spark.read().format("json")
                //.option("mode", "PERMISSIVE")  // 尽量解析
                .option("mode", "DROPMALFORMED")  // 丢弃不合法的数据
                .option("columnNameOfCorruptRecord", "error")
                .option("prefersDecimal", "true")  // 把double值优先解析成decimal
                .load("sql_data/datasource/order.data");
        ds3.show();
        ds3.createOrReplaceTempView("t_order");

        /*
            +-----+----+---+---+
            |  amt| oid|pid|uid|
            +-----+----+---+---+
            | 78.8| o_1|  1|  1|
            | 68.8| o_2|  2|  1|
            |160.0| o_3|  1|  1|
            |120.8| o_4|  2|  2|
            |780.8| o_5|  2|  2|
            | 78.8|o_10|  2|  4|
            | 68.8| o_6|  3|  2|
            | 78.8| o_7|  3|  3|
            | 78.8| o_9|  2|  4|
            | 78.8| o_8|  2|  3|
            +-----+----+---+---+
         */

        // 注册自定义函数导sql的会话中
        spark.udf().register("my_avg",new MyAvg());
        spark.sql("select uid,my_avg(amt) as amt from t_order group by uid").show();

    }


    public static class MyAvg extends UserDefinedAggregateFunction {


        // 描述函数 所要接收的 字段结构
        @Override
        public StructType inputSchema() {

            // 描述了一个输入字段的类型
            StructField field1 = DataTypes.createStructField("d1", DataTypes.DoubleType, true);

            // 将输入字段描述变成一个list
            List<StructField> structFields = Collections.singletonList(field1);

            // 再利用字段list创建了一个structType
            return DataTypes.createStructType(structFields);
        }

        // 描述函数聚合过程中使用的中间累加器结构
        @Override
        public StructType bufferSchema() {

            // 中间缓存器有两个字段： 输入数据的个数，输入数据的和

            StructField cntField = DataTypes.createStructField("cnt", DataTypes.IntegerType, true);
            StructField sumField = DataTypes.createStructField("sum", DataTypes.DoubleType, true);

            return DataTypes.createStructType(Arrays.asList(cntField, sumField));
        }

        // 函数的最终返回值数据类型
        @Override
        public DataType dataType() {
            return DataTypes.DoubleType;
        }

        // 确定性： 如果输入一组相同的数据，是否能得到相同的结果
        // 底层一些优化逻辑会看这个确定性来决定是否执行一些计算逻辑的优化措施
        @Override
        public boolean deterministic() {
            return true;
        }


        // 初始化中间累加器
        @Override
        public void initialize(MutableAggregationBuffer buffer) {

            // 初始化buffer（累加器）中的第0个字段：cnt
            buffer.update(0, 0);

            // 初始化buffer（累加器）中的第1个字段：sum
            buffer.update(1, 0.0);


        }


        // 聚合数据的逻辑
        @Override
        public void update(MutableAggregationBuffer buffer, Row input) {

            // cnt更新为：原来的值+1
            buffer.update(0, buffer.getInt(0) + 1);

            // sum更新为：原来的值 + 输入数据的值
            buffer.update(1, buffer.getDouble(1) + input.getDouble(0));


        }

        // 聚合两个累加器的逻辑
        @Override
        public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

            // cnt更新为： buffer1的cnt + buffer2的cnt
            buffer1.update(0,buffer1.getInt(0) + buffer2.getInt(0));

            // sum更新为： buffer1的sum + buffer2的sum
            buffer1.update(1,buffer1.getDouble(1) + buffer2.getDouble(1));

        }


        // 返回最终聚合值
        @Override
        public Double evaluate(Row buffer) {
            return Math.round(10*buffer.getDouble(1) / buffer.getInt(0))/10.0;
        }
    }

}
