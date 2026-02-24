package top.doe.web_mgmt.controller;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.math.RandomUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.roaringbitmap.longlong.Roaring64Bitmap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import redis.clients.jedis.Jedis;
import top.doe.web_mgmt.pojo.*;
import top.doe.web_mgmt.service.RuleMgmtService;
import top.doe.web_mgmt.utils.ConnectionFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;

@RestController
public class RuleMgmtController_Mock {
    Connection conn;

    @RequestMapping("/api/mock/get-tags")
    public TagOptsVo getTags() throws SQLException {

        List<TagOpt> list = new ArrayList<TagOpt>();

        list.add(new TagOpt("age", "年龄", "[20,60]", "numeric"));
        list.add(new TagOpt("gender", "性别", "male/female", "string"));
        list.add(new TagOpt("active_level", "活跃度", "{A,B,C}", "string"));
        list.add(new TagOpt("m_consume", "月消费", "[0,)", "numeric"));

        return new TagOptsVo(list);
    }


    @RequestMapping("/api/mock/crowd-selection")
    public CrowdQueryResVo queryCrowd(@RequestBody CrowdQueryParam queryParam) throws IOException {

        int i = RandomUtils.nextInt(100000);

        return new CrowdQueryResVo(i);
    }


    @RequestMapping("/api/mock/get-events")
    public OptionEventsVo queryEventOpts() throws SQLException {
        List<EventOpt> list = new ArrayList<>();
        list.add(new EventOpt(1, "添加购物车", Arrays.asList("item_id", "page_url")));
        list.add(new EventOpt(2, "收藏音乐", Arrays.asList("music_id", "page_url")));
        list.add(new EventOpt(3, "文章点赞", Arrays.asList("article_id", "page_url")));
        list.add(new EventOpt(4, "文章分享", Arrays.asList("article_id", "page_url")));


        return new OptionEventsVo(list);
    }


    //
    @RequestMapping("/api/mock/submit-rule-bak")
    public int submitRule1(@RequestBody String param) throws SQLException, IOException {

        if (conn == null) {
            conn = ConnectionFactory.getRuleMarketingMySqlConnection();
        }

        // 查询规则表中此刻的最大id
        PreparedStatement idPst = conn.prepareStatement("select max(id) as max_id from dw_50.rule_metadata");
        ResultSet idRs = idPst.executeQuery();
        int maxId = 0;
        while (idRs.next()) {
            maxId = idRs.getInt("max_id");
        }
        int ruleId = maxId + 1;

        JSONObject paramObj = JSONObject.parseObject(param);
        int modelId = paramObj.getIntValue("model_id");


        // 根据模型id，去查模型表中，该模型对应的运算机源代码
        PreparedStatement modelPst = conn.prepareStatement("select model_code,model_classname from dw_50.rule_model where id=?");
        modelPst.setInt(1, modelId);
        ResultSet rs = modelPst.executeQuery();
        String modelCode = "";
        String modelClassname = "";
        while (rs.next()) {
            modelCode = rs.getString("model_code");
            modelClassname = rs.getString("model_classname");
        }

        // 根据规则参数中的预圈选条件，去es查询人群
        // 人群圈选
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(1, 2, 5, 6, 8, 10, 12, 14);

        // 根据规则参数中的跨区统计条件，选取统计截止时间，然后去doris中进行统计
        long historyStatisticEndTime = System.currentTimeMillis();

        // 假装去doris查询完了,如下就是查询的假数据结果
        HashMap<Long, String> c1HistoryResult = new HashMap<>();
        c1HistoryResult.put(1L, "1_2");  // 序列总完成次数_索引到了几
        c1HistoryResult.put(2L, "2_1");
        c1HistoryResult.put(5L, "2_2");
        c1HistoryResult.put(6L, "3_1");
        c1HistoryResult.put(8L, "3_0");


        HashMap<Long, String> c2HistoryResult = new HashMap<>();
        c2HistoryResult.put(1L, "2_120");  // 事件完成了次数_总和
        c2HistoryResult.put(2L, "2_120");
        c2HistoryResult.put(5L, "2_160");
        c2HistoryResult.put(6L, "4_420");
        c2HistoryResult.put(8L, "4_420");


        // 把doris的查询结果发布到redis
        Jedis jedis = new Jedis("doitedu03", 6379);
        for (Map.Entry<Long, String> entry : c1HistoryResult.entrySet()) {
            Long userId = entry.getKey();
            String seqCnt_Idx = entry.getValue();
            // 写入redis
            if (bitmap.contains(userId)) {
                jedis.hset(ruleId + ":c1", String.valueOf(userId), seqCnt_Idx);
            }
        }


        for (Map.Entry<Long, String> entry : c2HistoryResult.entrySet()) {
            Long userId = entry.getKey();
            String cnt_sum = entry.getValue();
            // 写入redis
            if (bitmap.contains(userId)) {
                jedis.hset(ruleId + ":c2", String.valueOf(userId), cnt_sum);
            }
        }


        // 规则元数据的提交

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50", "root", "ABC123.abc123");

        paramObj.put("rule_id", ruleId);

        PreparedStatement pst = conn.prepareStatement("insert into dw_50.rule_metadata values(?,?,?,?,?,?,?,?,?,?,?,?)");
        pst.setInt(1, ruleId);
        pst.setString(2, "模型3测试规则" + ruleId);

        pst.setInt(3, modelId);

        pst.setString(4, modelCode);

        pst.setString(5, modelClassname);


        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(ba);
        bitmap.serialize(da);
        pst.setBytes(6, ba.toByteArray());

        pst.setString(7, paramObj.toJSONString());

        pst.setInt(8, 1);

        pst.setLong(9, historyStatisticEndTime);

        pst.setString(10, "小仙女");
        pst.setString(11, "小童子");
        pst.setInt(12, 1);

        pst.execute();

        return 1;
    }

    //
    @RequestMapping("/api/mock/submit-rule")
    public int submitRule2(@RequestBody String param) throws SQLException, IOException {

        if (conn == null) {
            conn = ConnectionFactory.getRuleMarketingMySqlConnection();
        }

        // 查询规则表中此刻的最大id
        PreparedStatement idPst = conn.prepareStatement("select max(id) as max_id from dw_50.rule_metadata");
        ResultSet idRs = idPst.executeQuery();
        int maxId = 0;
        while (idRs.next()) {
            maxId = idRs.getInt("max_id");
        }
        int ruleId = maxId + 1;

        JSONObject paramObj = JSONObject.parseObject(param);
        int modelId = paramObj.getIntValue("model_id");


        // 根据模型id，去查模型表中，该模型对应的运算机源代码
        PreparedStatement modelPst = conn.prepareStatement("select model_code,model_classname from dw_50.rule_model where id=?");
        modelPst.setInt(1, modelId);
        ResultSet rs = modelPst.executeQuery();
        String modelCode = "";
        String modelClassname = "";
        while (rs.next()) {
            modelCode = rs.getString("model_code");
            modelClassname = rs.getString("model_classname");
        }


        // 根据规则参数中的预圈选条件，去es查询人群
        // 人群圈选
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf();
        int crowdCnt = 30000 + RandomUtils.nextInt(10000);

        for (long i = 1; i <= crowdCnt; i++) {
            bitmap.add(i);
        }


        // 根据规则参数中的跨区统计条件，选取统计截止时间，然后去doris中进行统计
        long historyStatisticEndTime = System.currentTimeMillis();

        // 假装去doris查询完了,如下就是查询的假数据结果

        // 把doris的查询结果发布到redis
        Jedis jedis = new Jedis("doitedu03", 6379);
        for (int i = 0; i < crowdCnt; i++) {
            if(RandomUtils.nextInt(10) %3 != 0) {
                String seqCnt_Idx = (1 + RandomUtils.nextInt(6)) + "_" + (RandomUtils.nextInt(3));
                String cnt_sum = (1 + RandomUtils.nextInt(6)) + "_" + (10+RandomUtils.nextInt(500));
                jedis.hset(ruleId + ":c1", String.valueOf(i), seqCnt_Idx);
                jedis.hset(ruleId + ":c2", String.valueOf(i), cnt_sum);
            }
        }


        // 规则元数据的提交

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50", "root", "ABC123.abc123");

        paramObj.put("rule_id", ruleId);

        PreparedStatement pst = conn.prepareStatement("insert into dw_50.rule_metadata values(?,?,?,?,?,?,?,?,?,?,?,?)");
        pst.setInt(1, ruleId);
        pst.setString(2, "模型3测试规则" + ruleId);

        pst.setInt(3, modelId);

        pst.setString(4, modelCode);

        pst.setString(5, modelClassname);


        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(ba);
        bitmap.serialize(da);
        pst.setBytes(6, ba.toByteArray());

        pst.setString(7, paramObj.toJSONString());

        pst.setInt(8, 1);

        pst.setLong(9, historyStatisticEndTime);

        pst.setString(10, "小仙女");
        pst.setString(11, "小童子");
        pst.setInt(12, 1);

        pst.execute();

        return 1;
    }


    @RequestMapping("/api/mock/get_rules")
    public RuleInfoVo getRules() throws SQLException, IOException {
        if (conn == null) {
            conn = ConnectionFactory.getRuleMarketingMySqlConnection();
        }
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select * from dw_50.rule_metadata");
        ArrayList<RuleInfo> list = new ArrayList<>();


        while (rs.next()) {
            int id = rs.getInt("id");
            String ruleName = rs.getString("rule_name");
            int ruleModel = rs.getInt("rule_model_id");
            byte[] bytes = rs.getBytes("rule_crowd_bitmap_bytes");
            int crowedCnt = -1;
            if (bytes != null) {
                try {
                    Roaring64Bitmap bm = Roaring64Bitmap.bitmapOf();
                    bm.deserialize(ByteBuffer.wrap(bytes));
                    crowedCnt = bm.getIntCardinality();
                } catch (Exception e) {

                }
            }


            String param_json = rs.getString("rule_param_json");
            int rule_status = rs.getInt("rule_status");
            long endtime = rs.getLong("history_statistic_endtime");
            String creator = rs.getString("creator");
            String auditor = rs.getString("auditor");

            RuleInfo ruleInfo = new RuleInfo(id, ruleName, ruleModel, crowedCnt, param_json, rule_status, endtime, creator, auditor);
            list.add(ruleInfo);

        }

        return new RuleInfoVo(list);


    }


    @RequestMapping("/api/mock/changeStatus")
    public boolean changeStatus(int ruleId, int flag) throws SQLException {
        if (conn == null) {
            conn = ConnectionFactory.getRuleMarketingMySqlConnection();
        }

        PreparedStatement pst = conn.prepareStatement("update dw_50.rule_metadata set rule_status = ? where id = ?");
        pst.setInt(1, flag);
        pst.setInt(2, ruleId);
        pst.execute();

        return true;
    }


}
