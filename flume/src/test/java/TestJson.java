import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class TestJson {
    public static void main(String[] args) {

        String json = "{\"username\":\"a\",\"session_id\":\"s10\",\"event_id\":\"search\",\"action_time\":1711036401000,\"lat\":38.089969323508726,\"lng\":114.35731900345093,\"release_channel\":\"华为应用市场\",\"device_type\":\"mi8\",\"properties\":{\"keyword\":\"usb 移动固态\",\"search_id\":\"sc01\"}}";

        JSONObject obj = JSON.parseObject(json);
        String actionTime = obj.getString("action_time");
        System.out.println(actionTime);

    }
}
