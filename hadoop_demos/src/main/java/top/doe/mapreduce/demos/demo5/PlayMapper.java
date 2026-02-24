package top.doe.mapreduce.demos.demo5;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

public class PlayMapper extends Mapper<LongWritable, Text,Text,PlayBean> {
    HashSet<String>  set = new HashSet<>();
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, PlayBean>.Context context) throws IOException, InterruptedException {
        set.addAll(Arrays.asList("video_play","video_hb","video_pause","video_resume","video_stop"));
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, PlayBean>.Context context) throws IOException, InterruptedException {

        JSONObject jsonObject = JSON.parseObject(value.toString());
        int userId = jsonObject.getIntValue("user_id");
        String eventId = jsonObject.getString("event_id");
        long actionTime = jsonObject.getLongValue("action_time");
        String playId = jsonObject.getJSONObject("properties").getString("play_id");

        // 过滤视频播放相关事件以外的事件
        if(set.contains(eventId)) {
            context.write(new Text(userId + "_" + playId), new PlayBean(playId, userId, actionTime, eventId));
        }

    }
}
