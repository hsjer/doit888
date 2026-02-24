package top.doe.spark.exer;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.roaringbitmap.RoaringBitmap;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/9/19
 * @Desc: 学大数据，上多易教育
 * 漏斗模型分析
 * 路径： e03, e05, e06
 **/
public class X3_OnyarnTest {
    public static void main(String[] args) throws IOException {

        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 映射输入数据到RDD
        JavaRDD<String> rdd = sc.textFile(args[0]);
        // id, f1          f2           f3        f4         f5        f6          f7         f8
        // 333,eAmEYjibJJ,pssyRPWdlD,WEidSKhPxm,DrtJpUNDIH,0289875272,6117395654,1284155792,1975305895
        // 首先对数据做过滤：f2字段包含BBQ不要
        // 接着按照f2字段分组，求每组的  去重人数,f5的平均值,f6的总和,f7的最大值,f8的最小值
        JavaRDD<Bean> beanRdd = rdd.map(line -> {
            String[] split = line.split(",");
            return new Bean(
                    Integer.parseInt(split[0]),
                    split[1],
                    split[2],
                    split[3],
                    split[4],
                    Long.parseLong(split[5]),
                    Long.parseLong(split[6]),
                    Long.parseLong(split[7]),
                    Long.parseLong(split[8])

            );
        });

        // 过滤
        JavaRDD<Bean> filtered = beanRdd.filter(bean -> !bean.f2.toUpperCase().contains("BBQ"));

        // 分组聚合
        JavaPairRDD<String, Bean> kvRdd = filtered.mapToPair(bean -> new Tuple2<String, Bean>(bean.f2, bean));
        JavaPairRDD<String, Agg> aggRdd = kvRdd.aggregateByKey(
                new Agg(),
                (agg, bean) -> {
                    agg.bm.add(bean.id);
                    agg.f5_count++;
                    agg.f5_sum += bean.f5;
                    agg.f6_sum += bean.f6;
                    agg.f7_max = Math.max(agg.f7_max, bean.f7);
                    agg.f8_min = Math.min(agg.f8_min, bean.f8);

                    return agg;
                },


                (agg1, agg2) -> {
                    agg1.bm.or(agg2.bm);
                    agg1.f5_count += agg2.f5_count;
                    agg1.f5_sum += agg2.f5_sum;
                    agg1.f6_sum += agg2.f6_sum;
                    agg1.f7_max = Math.max(agg1.f7_max, agg2.f7_max);
                    agg1.f8_min = Math.min(agg1.f8_min, agg2.f8_min);
                    return agg1;
                }
        );

        // 得到最终结果
        JavaPairRDD<String, String> resRdd = aggRdd.mapValues(agg -> JSON.toJSONString(new Res(agg.bm.getCardinality(), agg.f5_sum / (double) agg.f5_count, agg.f6_sum, agg.f7_max, agg.f8_min)));

        resRdd.saveAsTextFile(args[1]);


    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Bean implements Serializable {
        private int id;
        private String f1;
        private String f2;
        private String f3;
        private String f4;
        private long f5;
        private long f6;
        private long f7;
        private long f8;

    }


    // 去重人数,f5的平均值,f6的总和,f7的最大值,f8的最小值
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Agg implements Serializable {
        private RoaringBitmap bm = RoaringBitmap.bitmapOf();
        private int f5_count = 0;
        private long f5_sum = 0;
        private long f6_sum = 0;
        private long f7_max = Long.MIN_VALUE;
        private long f8_min = Long.MAX_VALUE;
    }



    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Res implements Serializable {
        private int user_count;
        private double f5_avg ;
        private long f6_sum ;
        private long f7_max ;
        private long f8_min;
    }

}
