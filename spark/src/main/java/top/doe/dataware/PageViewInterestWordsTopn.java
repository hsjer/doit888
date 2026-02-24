package top.doe.dataware;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/13
 * @Desc: 学大数据，上多易教育
 *   用户画像：浏览兴趣词top50 标签开发
 **/
public class PageViewInterestWordsTopn {

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .appName("PageViewInterestWordsTopn")
                .master("local[1]")
                .enableHiveSupport()
                .getOrCreate();


        Dataset<Row> df = spark.read().table("dwd.user_action_log_detail").where("dt='20241112' and event_id = 'page_load' ");

        Dataset<LogBean> beanDs = df.as(Encoders.bean(LogBean.class));

        JavaRDD<LogBean> beanRdd = beanDs.toJavaRDD();


        JavaRDD<GuidWord> segmented = beanRdd.mapPartitions(new FlatMapFunction<Iterator<LogBean>, GuidWord>() {

            @Override
            public Iterator<GuidWord> call(Iterator<LogBean> logBeanIterator) throws Exception {

                StringReader stringReader = null;
                IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);
                ArrayList<GuidWord> words = new ArrayList<>();

                while (logBeanIterator.hasNext()) {
                    LogBean logBean = logBeanIterator.next();
                    long guid = logBean.getGuid();
                    String title = logBean.getProperties().get("title");

                    // 分词
                    stringReader = new StringReader(title);
                    ikSegmenter.reset(stringReader);

                    Lexeme lexeme = null;
                    while ((lexeme = ikSegmenter.next()) != null) {

                        // 单字词抛弃
                        String w = lexeme.getLexemeText();
                        if (w.length() > 1) {
                            words.add(new GuidWord(guid,w));
                        }
                    }
                }

                return words.iterator();
            }
        });



        // 统计，每个用户，每个词的浏览次数，并进而找出浏览次数最高的前50个词
        Dataset<Row> dataFrame = spark.createDataFrame(segmented, GuidWord.class);

        dataFrame.createTempView("tmp");


        spark.sql("insert into table dws.profile_interest_words partition(dt='20241112') " +
                "select\n" +
                "guid,\n" +
                "concat_ws('|',collect_list(word)) as view_words, \n" +
                "concat_ws('|',collect_list(word)) as search_words, \n" +
                "concat_ws('|',collect_list(word)) as favor_words, \n" +
                "concat_ws('|',collect_list(word)) as dianzan_words \n" +
                "from (\n" +
                "    select\n" +
                "    guid,\n" +
                "    word,\n" +
                "    row_number() over(partition by guid order by count(1) desc) as rn  \n" +
                "\n" +
                "    from tmp \n" +
                "    group by guid,word\n" +
                ") o \n" +
                "where rn<=10\n" +
                "group by guid");





        spark.close();



    }




    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class GuidWord implements Serializable {
        long guid;
        String word;
    }




    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class LogBean implements Serializable {
        long guid;
        String event_id;
        long action_time;
        Map<String,String> properties;
    }




    public static void test(String[] args) throws IOException {


        String sentence = "小米空调挂机1.5/2匹 巨省电,新一级能效?!节能变频冷暖& 智能自清洁 壁挂式卧室空调挂机 以旧换新 大1匹 一级能效 巨省电26GW/S1A1";
        StringReader sentenceReader = new StringReader(sentence);
        IKSegmenter ikSegmenter = new IKSegmenter(sentenceReader, true);

        Lexeme next ;
        ArrayList<String> words1 = new ArrayList<>();
        while( (next = ikSegmenter.next())!=null){
            words1.add(next.getLexemeText());
        }


        StringReader sentenceReader2 = new StringReader(sentence);
        IKSegmenter ikSegmenter2 = new IKSegmenter(sentenceReader2, false);

        Lexeme next2 ;
        ArrayList<String> words2 = new ArrayList<>();
        while( (next2 = ikSegmenter2.next())!=null){
            words2.add(next2.getLexemeText());
        }

        System.out.println(words1);
        System.out.println(words2);


    }

}
