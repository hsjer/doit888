package top.doe.spark.exer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.roaringbitmap.RoaringBitmap;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;

public class X1_UserActive {
    public static void main(String[] args) throws IOException {

        // 删掉输出目录，省的每次测试都要去手动删除
        FileUtils.deleteDirectory(new File("./spark_data/user_active/output"));


        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 映射输入数据到RDD
        JavaRDD<String> rdd = sc.textFile("./spark_data/user_active/input/a.txt");

        // 解析数据
        JavaRDD<UserAcc> rdd2 = rdd.map(line -> {
            String[] split = line.split(",");
            return new UserAcc(Integer.parseInt(split[0]),
                    split[2],
                    split[4],
                    Long.parseLong(split[5])
            );
        });
        rdd2.cache();


        // 需求1： 8月的活跃用户
        JavaRDD<Integer> res =
                rdd2
                        .filter(bean -> bean.getLoginDate().startsWith("2024-08"))
                        .map(bean -> bean.id)
                        .distinct();

        res.saveAsTextFile("./spark_data/user_active/output/output1/");


        // 需求2：- 各性别用户的总访问时长
        JavaPairRDD<String, Long> kvRDD =
                rdd2.mapToPair(bean -> new Tuple2<>(bean.gender, bean.stayLong));
        JavaPairRDD<String, Long> res2 = kvRDD.reduceByKey(Long::sum);
        res2.saveAsTextFile("./spark_data/user_active/output/output2/");

        // 需求3： 各性别用户的平均访问时长
        JavaPairRDD<String, Double> res3 = rdd2.mapToPair(bean -> new Tuple2<>(bean.gender, bean))
                .aggregateByKey(
                        new Agg(),
                        (agg, bean) -> {
                            agg.bm.add(bean.id);
                            agg.stayAmt += bean.stayLong;
                            return agg;
                        },
                        (agg1, agg2) -> {
                            agg1.bm.or(agg2.bm);
                            agg1.stayAmt += agg2.stayAmt;
                            return agg1;

                        }
                )
                .mapValues(agg -> (double) agg.stayAmt / agg.bm.getCardinality());
        res3.saveAsTextFile("./spark_data/user_active/output/output3/");


        // 需求4： 发生过连续3天及以上登录的用户
        JavaPairRDD<Integer, Integer> res4 =
                rdd2.mapToPair(bean -> new Tuple2<>(bean.id, bean.loginDate))
                        .distinct()
                        .groupByKey()   // JavaPairRDD<Integer, Iterable<String>>
                        .mapValues(iter -> {   // iter: 2024-08-01, 2024-08-02, 2024-08-03, 2024-08-06, 2024-08-07

                            ArrayList<String> lst = new ArrayList<>();
                            for (String s : iter) {
                                lst.add(s);
                            }
                            Collections.sort(lst);

                            int max = 0;
                            int tmp = 1;
                            for (int i = 0; i < lst.size() - 1; i++) {
                                String d_next = lst.get(i + 1);
                                String d = lst.get(i);
                                LocalDate day2 = LocalDate.parse(d_next);
                                LocalDate day1 = LocalDate.parse(d);

                                long between = ChronoUnit.DAYS.between(day1, day2);
                                if (between == 1) {
                                    tmp++;
                                } else {
                                    max = Math.max(tmp, max);
                                    tmp = 1;
                                }
                            }

                            // 最后比一次
                            max = Math.max(tmp, max);

                            return max;
                        })
                        .filter(tp->tp._2>2);

        res4.saveAsTextFile("./spark_data/user_active/output/output4/");


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserAcc implements Serializable {
        private int id;
        private String loginDate;
        private String gender;
        private long stayLong;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg implements Serializable {
        private RoaringBitmap bm = RoaringBitmap.bitmapOf();
        private long stayAmt = 0L;

    }


}
