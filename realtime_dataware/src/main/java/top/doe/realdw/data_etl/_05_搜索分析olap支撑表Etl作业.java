package top.doe.realdw.data_etl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava30.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava30.com.google.common.cache.CacheBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Collector;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import top.doe.realdw.utils.EnvUtil;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class _05_搜索分析olap支撑表Etl作业 {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = EnvUtil.getEnv();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);


        // 建表读dwd
        tenv.executeSql("create table dwd_events_kafka(      \n" +
                // 原始用户行为日志中的字段
                "     user_id bigint                         \n" +
                "    ,session_id         string              \n" +
                "    ,event_id           string              \n" +
                "    ,action_time        bigint              \n" +
                "    ,release_channel    string              \n" +
                "    ,device_type        string              \n" +
                "    ,properties         map<string,string>  \n" +
                "    ,member_level_id bigint                 \n" +
                "    ,province   string                      \n" +
                "    ,city       string                      \n" +
                "    ,region     string                      \n" +
                "    ,rt as to_timestamp_ltz(action_time,3)  \n" +
                "    ,watermark for rt as rt                 \n" +
                ") WITH (                                     \n" +
                "    'connector' = 'kafka',                   \n" +
                "    'topic' = 'dwd-events',                  \n" +
                "    'properties.bootstrap.servers' = 'doitedu01:9092',\n" +
                "    'properties.group.id' = 'doit50_g1',  \n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'value.format' = 'json',              \n" +
                "    'value.fields-include' = 'EXCEPT_KEY' \n" +
                ")                                                    ");


        // 一次搜索生命周期中的所有事件数据，聚合成一行
        tenv.createTemporaryFunction("get_null",GetNull.class);
        tenv.executeSql(
                         "create temporary view agg   as   " +
                         "with tmp as (                                                                                                "+
                         " select                                                                                                       "+
                        "     user_id,                                                                                                  "+
                        "     release_channel,                                                                                          "+
                        "     province,                                                                                                 "+
                        "     city,                                                                                                     "+
                        "     region,                                                                                                   "+
                        "     if(event_id = 'search',action_time,cast(get_null() as bigint))                             as    search_time,               "+
                        "     if(event_id = 'search_return',action_time,cast(get_null() as bigint))                      as    search_return_time,        "+
                        "     if(event_id = 'search_return',cast( properties['res_cnt'] as int),cast(get_null() as int))  as    res_cnt,                   "+
                        "     if(event_id = 'search_click',1,0)                                        as    click_cnt,                 "+
                        "     properties['search_id']                                                  as    search_id,                 "+
                        "     properties['keyword']                                                    as    keyword,                   "+
                        "     rt                                                                                                        "+
                        " from dwd_events_kafka                                                                                         "+
                        " where event_id in ('search','search_return','search_click')                                                   "+
                        " )                                                                                                             "+
                        "                                                                                                               "+
                        " select                                                                                                        "+
                        //"     window_start,                                                                                             "+
                        //"     window_end,                                                                                               "+
                        "     user_id,                                                                                                  "+
                        "     release_channel,                                                                                          "+
                        "     province,                                                                                                 "+
                        "     city,region,                                                                                              "+
                        "     search_id,                                                                                                "+
                        "     keyword,                                                                                                  "+
                        "     '' as  split_words,                                                                                       "+
                        "     '' as  similar_word,                                                                                      "+
                        "     max(search_time)    as search_time,                                                                       "+
                        "     max(search_return_time) as search_return_time,                                                            "+
                        "     max(res_cnt) as res_cnt,                                                                                  "+
                        "     sum(click_cnt) as click_cnt                                                                               "+
                        " from table(                                                                                                   "+
                        "     tumble(table tmp, descriptor(rt),interval '1' minute)                                                     "+
                        " )                                                                                                             "+
                        " group by window_start,window_end,user_id,release_channel,                                                     "+
                        " province,city,region,search_id,keyword                                                                        "
        );


        Table agg = tenv.from("agg");
        DataStream<AggBean> dataStream = tenv.toDataStream(agg, AggBean.class);


        // 分词和近义词的获取
        SingleOutputStreamOperator<AggBean> resStream = dataStream.keyBy(bean -> bean.getKeyword())
                .process(new KeyedProcessFunction<String, AggBean, AggBean>() {


                    Cache<String, String> cache;
                    CloseableHttpClient httpClient;
                    HashMap<String, String> jsonMap;
                    HttpPost post;


                    @Override
                    public void open(Configuration parameters) throws Exception {

                        // guava的缓存工具
                        cache = CacheBuilder.newBuilder()
                                .maximumSize(100) // 设置缓存的最大容量为100
                                .expireAfterWrite(30, TimeUnit.MINUTES) // 设置写入后10分钟过期
                                .expireAfterAccess(30, TimeUnit.MINUTES) // 设置最后一次访问后5分钟过期
                                .build();

                        httpClient = HttpClients.createDefault();
                        post = new HttpPost("http://doitedu01:8081/api/post/simwords");
                        post.addHeader("Content-Type", "application/json; charset=utf-8");
                        post.addHeader("Accept", "application/json; charset=utf-8");

                        jsonMap = new HashMap<>();

                    }

                    @Override
                    public void processElement(AggBean aggBean, KeyedProcessFunction<String, AggBean, AggBean>.Context ctx, Collector<AggBean> out) throws Exception {

                        String keyword = aggBean.getKeyword();

                        // 先去缓存找近义词和分词
                        String ifPresent = cache.getIfPresent(keyword);
                        if (ifPresent != null) {
                            // 咖啡|速溶 :  三合一速溶咖啡
                            String[] split = ifPresent.split(":");
                            String splitWords = split[0];
                            String similarWord = split[1];
                            aggBean.setSplit_words(splitWords);
                            aggBean.setSimilar_word(similarWord);

                            out.collect(aggBean);

                        } else {  // 如果缓存中没有，则请求公司的算法服务
                            jsonMap.put("origin", keyword);
                            // 设置请求体参数
                            post.setEntity(new StringEntity(JSON.toJSONString(jsonMap), "utf-8"));
                            // 发起请求
                            CloseableHttpResponse response = httpClient.execute(post);
                            // 解析结果
                            HttpEntity entity = response.getEntity();
                            String resJson = EntityUtils.toString(entity);
                            JSONObject jsonObject = JSON.parseObject(resJson);
                            String split_words = jsonObject.getString("words");
                            String similar_word = jsonObject.getString("similarWord");

                            // 把请求的结果，放入缓存
                            cache.put(keyword, split_words + ":" + similar_word);

                            // 返回结果
                            aggBean.setSplit_words(split_words);
                            aggBean.setSimilar_word(similar_word);

                            out.collect(aggBean);

                        }

                    }
                });


        // 流转表
        tenv.createTemporaryView("res",resStream);

        tenv.executeSql("select * from res").print();



        env.execute();


    }


    public static class GetNull extends ScalarFunction implements Serializable {
        public String eval() {
            return null;
        }

    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AggBean implements Serializable {

        private Long user_id;
        private String release_channel;
        private String province;
        private String city,region;
        private String search_id;
        private String keyword;
        private String split_words;
        private String similar_word;
        private Long search_time;
        private Long search_return_time;
        private Long res_cnt;
        private Long click_cnt;



    }


}
