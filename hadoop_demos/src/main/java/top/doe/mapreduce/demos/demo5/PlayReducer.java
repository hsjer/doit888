package top.doe.mapreduce.demos.demo5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PlayReducer extends Reducer<Text, PlayBean, Text, LongWritable> {


    @Override
    protected void reduce(Text key, Iterable<PlayBean> values, Reducer<Text, PlayBean, Text, LongWritable>.Context context) throws IOException, InterruptedException {

        /*
         * 拿到的一组数据规律如下：
         * user1,play1,video_play,t11   => a
         * user1,play1,video_hb,t12
         * user1,play1,video_hb,t13
         * user1,play1,video_pause,t14  => b
         *
         * user1,play1,video_resume,t30  => c
         * user1,play1,video_hb,t31
         * user1,play1,video_hb,t32
         * user1,play1,video_pause,t33   => d
         *
         * user1,play1,video_resume,t41  => e
         * user1,play1,video_hb,t42
         * user1,play1,video_hb,t43
         * user1,play1,video_stop,t48    => f
         *
         * 算法1：   (b-a) + (d-c) + (f-e)
         * 算法2：   (b+d+f) - (a+c+e)
         */

        long startSum = 0;
        long endSum = 0;

        for (PlayBean value : values) {
            if (value.getEventId().equals("video_play") || value.getEventId().equals("video_resume")) {
                startSum += value.getActionTime();
            }

            if (value.getEventId().equals("video_pause") || value.getEventId().equals("video_stop")) {
                endSum += value.getActionTime();
            }
        }

        // key:   userId_playId
        // value: 播放时长
        context.write(key, new LongWritable(endSum - startSum));
    }
}
