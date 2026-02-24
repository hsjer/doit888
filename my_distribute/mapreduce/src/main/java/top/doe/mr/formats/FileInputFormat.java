package top.doe.mr.formats;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.Random;

public class FileInputFormat implements InputFormat {

    @Override
    public RecordReader createRecordReader(String infoJson) {

        // {
        //        "task_id":1,
        //        "map_task_splits":[
        //              {
        //                  "task_id":0,
        //                  "split_start":1,
        //                  "split_end":100
        //              },
        //              {
        //                  "task_id":1,
        //                  "split_start":101,
        //                  "split_end":200
        //              }
        //         ],
        // }
        JSONObject infoObj = JSONObject.parseObject(infoJson);

        String inputPath = infoObj.getString("input_path");

        int taskId = infoObj.getIntValue("task_id");

        JSONArray splitsArray = infoObj.getJSONArray("map_task_splits");

        int start = 0;
        int end = 0;
        for (int i = 0; i < splitsArray.size(); i++) {
            JSONObject iObj = splitsArray.getJSONObject(i);
            if(iObj.getIntValue("task_id") == taskId){
                start = iObj.getIntValue("split_start");
                end = iObj.getIntValue("split_end");
                break;
            }

        }

        return new FileRecordReader(inputPath, start,end);
    }


    public static class FileRecordReader implements RecordReader {

        String path;
        int start;
        int end;
        String[] events = {"add_cart","page_view","search","ad_show","ad_click","favor_collect","share"};
        Random random = new Random();

        JSONObject dataObj = new JSONObject();

        int count = 0;

        public FileRecordReader(String path, int start, int end) {
            this.path = path;
            this.start = start;
            this.end = end;
        }


        @Override
        public boolean hasNext() {

            return count<(end-start);
        }

        // {"user_id":1,"event_id":"add_cart"}
        @Override
        public String next() {

            dataObj.put("user_id",start+random.nextInt(end-start));
            dataObj.put("event_id",events[random.nextInt(events.length)]);

            count++;
            return dataObj.toJSONString();
        }
    }

}
