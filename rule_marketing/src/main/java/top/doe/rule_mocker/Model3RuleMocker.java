package top.doe.rule_mocker;

import org.roaringbitmap.longlong.Roaring64Bitmap;
import redis.clients.jedis.Jedis;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Model3RuleMocker {

    public static void main(String[] args) throws SQLException, IOException {

        String param = "{\n" +
                "  \"rule_id\": 4,\n" +
                "  \"model_id\": 3,\n" +
                "  \"static_profile_condition\": [\n" +
                "    {\n" +
                "      \"tag_name\": \"age\",\n" +
                "      \"tag_oper\": \">\",\n" +
                "      \"tag_value\": \"30\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"tag_name\": \"gender\",\n" +
                "      \"tag_oper\": \"=\",\n" +
                "      \"tag_value\": \"male\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"cross_range_realtime_condition\": [\n" +
                "    {\n" +
                "      \"start_time\": 1730390400000,\n" +
                "      \"end_time\": -1,\n" +
                "      \"event_seq\": [\n" +
                "        {\n" +
                "          \"event_id\": \"A\",\n" +
                "          \"props\": [\n" +
                "            {\n" +
                "              \"prop_name\": \"p1\",\n" +
                "              \"prop_oper\": \"contain\",\n" +
                "              \"prop_value\": \"咖啡\"\n" +
                "            }\n" +
                "          ]\n" +
                "        },\n" +
                "        {\n" +
                "          \"event_id\": \"E\",\n" +
                "          \"props\": [\n" +
                "            {\n" +
                "              \"prop_name\": \"p2\",\n" +
                "              \"prop_oper\": \">\",\n" +
                "              \"prop_value\": 200\n" +
                "            }\n" +
                "          ]\n" +
                "        },\n" +
                "        {\n" +
                "          \"event_id\": \"Q\",\n" +
                "          \"props\": []\n" +
                "        }\n" +
                "      ],\n" +
                "      \"seq_cnt_oper\": \">=\",\n" +
                "      \"seq_cnt_value\": 3,\n" +
                "      \"cross_condition_id\": \"c1\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"start_time\": 1730390400000,\n" +
                "      \"end_time\": -1,\n" +
                "      \"event_id\": \"W\",\n" +
                "      \"event_cnt_oper\": \">\",\n" +
                "      \"event_cnt_value\": 4,\n" +
                "      \"prop_name\": \"p1\",\n" +
                "      \"prop_oper\": \">\",\n" +
                "      \"prop_avg_value\": 150,\n" +
                "      \"cross_condition_id\": \"c2\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"fire_event_condition\": {\n" +
                "    \"event_id\": \"X\",\n" +
                "    \"prop_name\": \"p4\",\n" +
                "    \"prop_oper\": \">\",\n" +
                "    \"prop_value\": 20\n" +
                "\n" +
                "  }\n" +
                "}";

        Jedis jedis = new Jedis("doitedu03", 6379);

        HashMap<Long, String> c1HistoryResult = new HashMap<>();
        c1HistoryResult.put(1L, "1_2");
        c1HistoryResult.put(2L, "2_1");
        c1HistoryResult.put(5L, "2_2");
        c1HistoryResult.put(6L, "3_1");
        c1HistoryResult.put(8L, "3_0");


        HashMap<Long, String> c2HistoryResult = new HashMap<>();
        c2HistoryResult.put(1L, "2_120");
        c2HistoryResult.put(2L, "2_120");
        c2HistoryResult.put(5L, "2_160");
        c2HistoryResult.put(6L, "4_420");
        c2HistoryResult.put(8L, "4_420");



        // 人群圈选
        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(1, 2, 5, 6, 8, 10, 12, 14);

        // 历史数据统计  和 redis数据的发布
        long historyStatisticEndTime = System.currentTimeMillis();
/*        Connection dorisConn = DriverManager.getConnection("jdbc:mysql://doitedu01:8030/ods", "root", "123456");
        PreparedStatement pst = dorisConn.prepareStatement(
                " SELECT                                                                         " +
                        "    user_id,                                                                    " +
                        "    my_udf1(arr) as seq_cnt,                                                    " +
                        "    my_udf2(arr) as seq_idx                                                     " +
                        " FROM (                                                                         " +
                        "     SELECT                                                                     " +
                        "         user_id,                                                               " +
                        "         sort_array(collect_list(concat_ws('_',action_time,event_id))) as arr,  " +
                        "     from ods.user_action_log                                                   " +
                        "     where  start_time>=1730390400000   and  end_time<=?                        " +
                        "     AND (                                                                      " +
                        "         (event_id = 'A' and properties['p1'] like '%咖啡%')                    " +
                        "         OR                                                                     " +
                        "         (event_id = 'E' and cast(properties['p2'] as double) > 200)            " +
                        "         OR                                                                     " +
                        "         (event_id = 'Q')                                                       " +
                        "     )                                                                          " +
                        "     GROUP BY user_id                                                           " +
                        " ) o                                                                            "
        );
        pst.setLong(1, historyStatisticEndTime);
        ResultSet rs = pst.executeQuery();


        while(rs.next()) {
            long userId = rs.getLong("user_id");
            int seqCnt = rs.getInt("seq_cnt");
            int seqIdx = rs.getInt("seq_idx");

            // 写入redis
            if(bitmap.contains(userId)){
                jedis.hset("4:c1",String.valueOf(userId),seqCnt+"_"+seqIdx);
            }
        }*/


        for (Map.Entry<Long, String> entry : c1HistoryResult.entrySet()) {
            Long userId = entry.getKey();
            String seqCnt_Idx = entry.getValue();
            // 写入redis
            if(bitmap.contains(userId)){
                jedis.hset("4:c1",String.valueOf(userId),seqCnt_Idx);
            }
        }


        for (Map.Entry<Long, String> entry : c2HistoryResult.entrySet()) {
            Long userId = entry.getKey();
            String cnt_sum = entry.getValue();
            // 写入redis
            if(bitmap.contains(userId)){
                jedis.hset("4:c2",String.valueOf(userId),cnt_sum);
            }
        }


        // 规则元数据的提交

        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50", "root", "ABC123.abc123");

        PreparedStatement pst = conn.prepareStatement("insert into dw_50.rule_metadata values(?,?,?,?,?,?,?,?,?,?)");
        pst.setInt(1, 4);
        pst.setString(2, "测试规则2");
        pst.setInt(3, 1);


        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(ba);
        bitmap.serialize(da);
        pst.setBytes(4, ba.toByteArray());

        pst.setString(5, param);
        pst.setInt(6,1);

        pst.setLong(7, historyStatisticEndTime);

        pst.setString(8, "小仙女");
        pst.setString(9, "小童子");
        pst.setInt(10, 1);

        pst.execute();

    }

}
