package top.doe.dataware;

import com.alibaba.fastjson.JSON;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/13
 * @Desc: 学大数据，上多易教育
 *   页面下游流量贡献量计算
 **/
public class TrafficContributeProcess {
    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .appName("TrafficContributeProcess")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();


        // 1,t1,page_load,properties{url:/a ,ref:null}
        Dataset<Row> ds = spark.sql("select guid,session_id,action_time,event_id,page_url,properties['ref'] as ref_page_url  from tmp.user_actionlog_detail_traffic where dt='20241101'");

        // 把dataset<row> 转成 rdd<PageEvent>
        JavaRDD<Row> rddRow = ds.toJavaRDD();
        JavaRDD<PageEvent> rddPageEvent = rddRow.map(new Function<Row, PageEvent>() {
            @Override
            public PageEvent call(Row row) throws Exception {
                long guid = row.getAs("guid");
                String session_id = row.getAs("session_id");
                long action_time = row.getAs("action_time");
                String event_id = row.getAs("event_id");
                String page_url = row.getAs("page_url");
                String ref_page_url = row.getAs("ref_page_url");

                return new PageEvent(guid, session_id, action_time, event_id, page_url, ref_page_url);
            }
        });

        // 按相同会话将数据分组
        JavaPairRDD<String, Iterable<PageEvent>> grouped = rddPageEvent.mapToPair(new PairFunction<PageEvent, String, PageEvent>() {

            @Override
            public Tuple2<String, PageEvent> call(PageEvent pageEvent) throws Exception {

                String key = pageEvent.getGuid() + "|" + pageEvent.getSession_id();

                return Tuple2.apply(key, pageEvent);
            }
        }).groupByKey();


        // 核心计算逻辑
        JavaRDD<ArrayList<ContributeInfo>> coreResults = grouped.map(tp -> {
            ArrayList<PageEvent> lst = new ArrayList<>();
            for (PageEvent pageEvent : tp._2) {
                lst.add(pageEvent);
            }

            // 把页面浏览事件按照时间先后排序
            Collections.sort(lst, new Comparator<PageEvent>() {
                @Override
                public int compare(PageEvent o1, PageEvent o2) {
                    return Long.compare(o1.getAction_time(), o2.getAction_time());
                }
            });

            // 构造树
            // 从list的末尾开始，为每个节点找他的父节点
            for (int i = lst.size() - 1; i > 0; i--) {
                PageEvent currNode = lst.get(i);
                String refPageUrl = currNode.getRef_page_url();
                // 到节点list中去找，page_url=refPageUrl的节点
                for (int j = i - 1; j >= 0; j--) {
                    PageEvent nodeJ = lst.get(j);
                    if (refPageUrl.equals(nodeJ.getPage_url())) {
                        // 给j节点的children 列表，添加当前节点作为一个子
                        nodeJ.getChildren().add(currNode);
                        break;
                    }
                }
            }


            // 循环走完之后，根节点就代表了整棵树
            PageEvent rootNode = lst.get(0);

            // 计算 这个会话中，每个页面的 直接贡献量  和   下游总贡献量
            ArrayList<ContributeInfo> results = new ArrayList<>();
            trafficContribute(rootNode, results);


            // 循环走完之后，根节点就代表了整棵树
            return results;
        });


        // 压平上面的结果
        // RDD {
        //    List[a:3:10 ,  b:2:5  , f:10:25 ]
        //    List[a:4:12 ,  c:6:15  ,q:11:22 ]
        //    List[a:3:16 ,  b:8:25  ,f:20:45 ]
        // }

        /**
         * (guid=1, session_id=s01, page_url=/d, directContribute=0, wholeContribute=0)
         * (guid=1, session_id=s01, page_url=/w, directContribute=0, wholeContribute=0)
         * (guid=1, session_id=s01, page_url=/a, directContribute=0, wholeContribute=0)
         * (guid=1, session_id=s01, page_url=/f, directContribute=2, wholeContribute=2)
         * (guid=1, session_id=s01, page_url=/e, directContribute=0, wholeContribute=0)
         * (guid=1, session_id=s01, page_url=/c, directContribute=2, wholeContribute=4)
         * (guid=1, session_id=s01, page_url=/b, directContribute=0, wholeContribute=0)
         * (guid=1, session_id=s01, page_url=/a, directContribute=3, wholeContribute=7)
         *
         * (guid=2, session_id=s02, page_url=/d, directContribute=0, wholeContribute=0)
         * (guid=2, session_id=s02, page_url=/w, directContribute=0, wholeContribute=0)
         * (guid=2, session_id=s02, page_url=/a, directContribute=0, wholeContribute=0)
         * (guid=2, session_id=s02, page_url=/f, directContribute=2, wholeContribute=2)
         * (guid=2, session_id=s02, page_url=/e, directContribute=0, wholeContribute=0)
         * (guid=2, session_id=s02, page_url=/c, directContribute=2, wholeContribute=4)
         * (guid=2, session_id=s02, page_url=/b, directContribute=0, wholeContribute=0)
         * (guid=2, session_id=s02, page_url=/a, directContribute=3, wholeContribute=7)
         */

        JavaRDD<ContributeInfo> resultRDD = coreResults.flatMap(ArrayList::iterator);


        // 进行每个页面的全局聚合：每个页面的直接贡献、总贡献
        Dataset<Row> df = spark.createDataFrame(resultRDD, ContributeInfo.class);
        df.createTempView("tmp");

        spark.sql("insert into table tmp.page_traffic_contribute partition(dt='20241101') " +
                "select " +
                "page_url,sum(directContribute) as direct_contribute," +
                "sum(wholeContribute) as whole_contribute  " +
                "from tmp " +
                "group by page_url");


        spark.close();


    }


    // 总贡献量 =  子节点数  + 每个子节点的总贡献量
    public static int trafficContribute(PageEvent node ,List<ContributeInfo> results ) {

        int wholeContribute = 0;

        // 计算当前节点的：直接贡献量
        List<PageEvent> children = node.children;
        int directContribute = children.size();
        wholeContribute += directContribute;

        // 计算每个子节点的总贡献,并累加到当前节点的：总贡献
        for (PageEvent childNode : children) {
            int childWholeContribute = trafficContribute(childNode,results);
            wholeContribute += childWholeContribute;
        }

        // 把当前正在计算的node的结果信息，添加到结果list中
        ContributeInfo contributeInfo = new ContributeInfo(node.guid, node.session_id, node.page_url, directContribute, wholeContribute);
        results.add(contributeInfo);

        return wholeContribute;

    }





    @Data
    @NoArgsConstructor
    public static class PageEvent implements Serializable {
        private long guid;
        private String session_id;
        private long action_time;
        private String event_id;
        private String page_url;
        private String ref_page_url;
        private List<PageEvent> children;

        public PageEvent(long guid, String session_id, long action_time, String event_id, String page_url, String ref_page_url) {
            this.guid = guid;
            this.session_id = session_id;
            this.action_time = action_time;
            this.event_id = event_id;
            this.page_url = page_url;
            this.ref_page_url = ref_page_url;
            this.children = new ArrayList<>();
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ContributeInfo implements Serializable {
        private long guid;
        private String session_id;
        private String page_url;
        private int directContribute;
        private int wholeContribute;

    }


}
