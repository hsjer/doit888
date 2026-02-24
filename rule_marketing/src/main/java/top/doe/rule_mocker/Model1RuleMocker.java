package top.doe.rule_mocker;

import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Model1RuleMocker {

    public static void main(String[] args) throws SQLException, IOException {

        String param = "{\n" +
                "  \"rule_id\":2,\n" +
                "  \"static_profile_condition\":[\n" +
                "    {\n" +
                "      \"tag_name\":\"职业\",\n" +
                "      \"operator\":\"=\",\n" +
                "      \"tag_value\":[\"宝妈\"]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tag_name\":\"年龄\",\n" +
                "      \"operator\":\"BETWEEN\",\n" +
                "      \"tag_value\":[20,30]\n" +
                "    },\n" +
                "    {\n" +
                "      \"tag_name\":\"月消费额\",\n" +
                "      \"operator\":\">\",\n" +
                "      \"tag_value\":[1000]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"realtime_profile_condition\":{\n" +
                "    \"event_id\":\"B\",\n" +
                "    \"cnt_operator\":\">\",\n" +
                "    \"event_cnt\":4,\n" +
                "    \"prop_name\":\"p1\",\n" +
                "    \"avg_prop_value\":100,\n" +
                "    \"min_prop_value\":50,\n" +
                "    \"max_prop_value\":200\n" +
                "  },\n" +
                "  \"fire_event\": {\n" +
                "    \"event_id\": \"Y\",\n" +
                "    \"props\": [\n" +
                "      {\n" +
                "        \"prop_name\": \"p2\",\n" +
                "        \"operator\": \">\",\n" +
                "        \"prop_value\":[\"10\"]\n" +
                "      }\n" +
                "    ]\n" +
                "  }\n" +
                "}";


        Connection conn = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50", "root", "ABC123.abc123");

        PreparedStatement pst = conn.prepareStatement("insert into dw_50.rule_metadata values(?,?,?,?,?,?,?,?,?)");
        pst.setInt(1, 2);
        pst.setString(2, "测试规则2");
        pst.setInt(3, 1);

        Roaring64Bitmap bitmap = Roaring64Bitmap.bitmapOf(1, 2, 5, 6, 8, 10, 12, 14);
        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        DataOutputStream da = new DataOutputStream(ba);
        bitmap.serialize(da);
        pst.setBytes(4, ba.toByteArray());

        pst.setString(5, param);
        pst.setInt(6,1);
        pst.setString(7, "小仙女");
        pst.setString(8, "小童子");
        pst.setInt(9, 1);

        pst.execute();


    }

}
