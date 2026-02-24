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
 *    漏斗模型分析
 *    路径： e03, e05, e06
 **/
public class X2_FunnelModel {
    public static void main(String[] args) throws IOException {


        // 删掉输出目录，省的每次测试都要去手动删除
        FileUtils.deleteDirectory(new File("./spark_data/funnel/output"));


        SparkConf conf = new SparkConf();
        conf.setAppName("xxx");
        conf.setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // 映射输入数据到RDD
        JavaRDD<String> rdd = sc.textFile("./spark_data/funnel/input/a.txt");

        // 解析json
        JavaPairRDD<Integer, UserEvent> kvRDD = rdd.mapToPair(s -> {
            UserEvent userEvent = JSON.parseObject(s, UserEvent.class);
            return new Tuple2<Integer, UserEvent>(userEvent.uid, userEvent);
        });

        // 按用户分组
        JavaPairRDD<Integer, Iterable<UserEvent>> groupedRDD = kvRDD.groupByKey();

        // 对每组计算
        JavaPairRDD<Integer, String> tmp = groupedRDD.flatMapValues(new FlatMapFunction<Iterable<UserEvent>, String>() {
            @Override
            public Iterator<String> call(Iterable<UserEvent> userEvents) throws Exception {

                ArrayList<UserEvent> lst = new ArrayList<>();
                // 1,e03,t1
                // 1,e05,t3
                // 1,e02,t2
                for (UserEvent userEvent : userEvents) {
                    lst.add(userEvent);
                }

                //  按事件的发生时间排序
                Collections.sort(lst, Comparator.comparingLong(o -> o.event_time));
                // 1,e03,t1
                // 1,e02,t2
                // 1,e05,t3
                // 1,e04,t4
                // 1,e06,t8
                ArrayList<String> resList = new ArrayList<>();
                String[] steps = {"e03", "e05", "e06"};
                int i = 0;
                for (UserEvent userEvent : lst) {
                    if (userEvent.event_id.equals(steps[i])) {
                        resList.add("step_" + (++i));   // 产生一个结果，并递增i
                        if (i >= 3) break;   // 已经找完了最大步骤，不需要继续找了
                    }
                }
                return resList.iterator();
            }
        });

        // 把上面的结果（人，步骤）  ，变成 （步骤，1）
        JavaPairRDD<String, Integer> stepPair = tmp.mapToPair(tp -> new Tuple2<String, Integer>(tp._2, 1));

        // 对上面的结果按步骤来累加1，得到每个步骤的人数
        JavaPairRDD<String, Integer> stepUserCount = stepPair.reduceByKey(Integer::sum);

        // 把上面的分布式计算结果，收集到driver端，用单机程序计算出最终结果
        List<Tuple2<String, Integer>> fakeLst = stepUserCount.collect();

        ArrayList<Tuple2<String, Integer>> realList = new ArrayList<>();
        for (Tuple2<String, Integer> tp : fakeLst) {
            realList.add(tp);
        }



        Collections.sort(realList, new Comparator<Tuple2<String, Integer>>() {
            @Override
            public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {

                return o1._1.compareTo(o2._1);
            }
        });

        Tuple2<String, Integer> tpFirst = realList.get(0);

        for (int i = 0; i < realList.size(); i++) {
            Tuple2<String, Integer> tpCur = realList.get(i);  // 当前的步骤的人数
            Tuple2<String, Integer> tpPre = realList.get(Math.max(0,i-1));  // 前一个步骤的人数

            double xd = tpCur._2/(double)tpPre._2;
            double jd = tpCur._2/(double)tpFirst._2;

            System.out.println(String.format("步骤号：%s, 触达人数: %d, 绝对转化率: %.2f, 相对转化率: %.2f",tpCur._1,tpCur._2,jd,xd));
        }

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserEvent implements Serializable {
        // {"uid":1,"event_time":1725886211000,"event_id":"e03","properties":{"url":"/aaa/bbb"},"device_type":"android"}

        private int uid;
        private long event_time;
        private String event_id;
    }

}
