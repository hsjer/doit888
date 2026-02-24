package cn.doitedu.mr;

import com.alibaba.fastjson.JSONObject;
import top.doe.mr.map.Mapper;

public class JsonParseMapper implements Mapper {
    @Override
    public String map(String data) {

        /*try {
            Thread.sleep(6000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }*/

        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < 60000){
            Math.pow(Math.random(),Math.random());
        }


        // {"user_id":1,"event_id":"add_cart"}
        JSONObject jsonObject = JSONObject.parseObject(data);
        String eventId = jsonObject.getString("event_id");
        int userId = jsonObject.getIntValue("user_id");
        if(eventId.equals("add_cart")){
            return userId+","+eventId;
        }

        return userId+",filtered_event";
    }
}
