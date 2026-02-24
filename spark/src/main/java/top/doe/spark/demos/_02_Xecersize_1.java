package top.doe.spark.demos;


import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/11
 * @Desc: 学大数据，上多易教育
 * 有如下数据：<br />
 * a,1
 * a,1
 * a,b
 * a,5
 * b,6
 * b,10
 * b,x
 * a,10
 * b,26
 * <p>
 * <br />处理数据，得到如下结果：<br />
 * a,[11b510]   <br />
 * b,[610x26]
 **/
public class _02_Xecersize_1 {


    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setMaster("local");
        conf.setAppName("name");


        JavaSparkContext sc = new JavaSparkContext(conf);


        List<String> datas = Arrays.asList(
                "a,1",
                "b,1",
                "b,2",
                "a,2",
                "a,3",
                "a,4",
                "a,5",
                "b,3",
                "b,4"
        );

        // parallelize 是测试用的算子 => 用于把一个内存集合转换成一个RDD
        JavaRDD<String> rdd1 = sc.parallelize(datas);

        // 先把数据变成KV形式
        JavaPairRDD<String, String> rdd2 = rdd1.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] split = s.split(",");
                return Tuple2.apply(split[0], split[1]);
            }
        });

        // 接下来按key聚合
        // 把字符串聚合到一个List中去

        // 结果：a,List[1,2,3,4,5]
        JavaPairRDD<String, ArrayList<String>> rdd3 = rdd2.aggregateByKey(
                new ArrayList<String>(),
                new Function2<ArrayList<String>, String, ArrayList<String>>() {
                    @Override
                    public ArrayList<String> call(ArrayList<String> agg, String v2) throws Exception {
                        agg.add(v2);
                        return agg;
                    }
                },
                new Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>() {
                    @Override
                    public ArrayList<String> call(ArrayList<String> agg, ArrayList<String> partialAgg) throws Exception {
                        agg.addAll(partialAgg);
                        return agg;
                    }
                }

        );

        // (a,List[1,2,3,4,5]) =>  a,[12345]
        JavaRDD<String> resRDD = rdd3.map(new Function<Tuple2<String, ArrayList<String>>, String>() {
            @Override
            public String call(Tuple2<String, ArrayList<String>> tp2) throws Exception {
                String key = tp2._1;
                ArrayList<String> value = tp2._2;
                return key + "," + "[" + StringUtils.join(value, "") + "]";
            }
        });


        // foreach是一个行动算子： 对rdd中的每一条数据施加一个VoidFunction
        resRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });



    }


}
