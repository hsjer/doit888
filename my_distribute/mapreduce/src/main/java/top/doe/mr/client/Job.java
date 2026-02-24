package top.doe.mr.client;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import top.doe.common.yarn.AppLaunchContext;
import top.doe.common.yarn.AppLaunchResp;
import top.doe.common.yarn.ApplicationRequest;
import top.doe.common.yarn.ApplicationResponse;
import top.doe.mr.YarnChild;
import top.doe.mr.formats.InputFormat;
import top.doe.mr.map.Mapper;
import top.doe.mr.reduce.Reducer;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class Job {

    JSONObject jobInfoJson = new JSONObject();
    private String jarPath;
    private String frameworkName;
    private int mapTaskNumb;


    public void setMapperClass(Class<? extends Mapper> mapperClass) {

        jobInfoJson.put("userMapperName", mapperClass.getName());
    }

    public void setReducerClass(Class<? extends Reducer> reducerClass) {

        jobInfoJson.put("userReducerName", reducerClass.getName());
    }

    public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
        jobInfoJson.put("input_format", inputFormatClass.getName());
    }

    public void setJarPath(String jarPath) {
        jobInfoJson.put("jarPath", jarPath);
        this.jarPath = jarPath;

    }

    public void setOutputPath(String outputPath) {
        jobInfoJson.put("output_path", outputPath);
    }

    public void setInputPath(String inputPath) {
        jobInfoJson.put("input_path", inputPath);
    }

    public void setMapTaskNumb(int mapTaskNumb) {
        this.mapTaskNumb = mapTaskNumb;
        jobInfoJson.put("map_task_num", mapTaskNumb);

    }


    public void setReduceTaskNumb(int reduceTaskNumb) {
        jobInfoJson.put("reduce_task_num", reduceTaskNumb);
    }


    // ["0:1,100","1:101,200"]

    // 现实中的
    // /aa/input/a.txt ,  startPosition: 0 , length:100M
    // /aa/input/a.txt ,  startPosition: 100M , length:100M
    public void setMapTaskSplits(String[] splits) {

        JSONArray splitArray = new JSONArray();

        for (String splitStr : splits) {
            String[] fields = splitStr.split(":");
            String[] startAndEnd = fields[1].split(",");


            JSONObject splitObj = new JSONObject();
            splitObj.put("task_id", Integer.parseInt(fields[0]));
            splitObj.put("split_start", Integer.parseInt(startAndEnd[0]));
            splitObj.put("split_end", Integer.parseInt(startAndEnd[1]));

            splitArray.add(splitObj);

        }

        jobInfoJson.put("map_task_splits", splitArray);

        /**
         * 最终形成的结构:
         *
         * {
         *     "map_task_splits":[
         *          {
         *              "task_id":0,
         *              "split_start":1,
         *              "split_end":100
         *          },
         *          {
         *              "task_id":1,
         *              "split_start":101,
         *              "split_end":200
         *          }
         *     ]
         * }
         */
    }

    public void submit(String mapreduce_framework_name) throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        if ("local".equals(mapreduce_framework_name)) {

            // 获取 mapTask 的 并行度
            int mapTaskNum = this.jobInfoJson.getIntValue("map_task_num");

            // 给作业参数填充job_id
            this.jobInfoJson.put("job_id", "local-job");
            // 填充要提交的任务阶段
            this.jobInfoJson.put("phase", "map");

            for (int i = 0; i < mapTaskNum; i++) {

                // 给作业参数填充 task_id

                this.jobInfoJson.put("task_id", i);


                // 调 yarn child 去执行 MapTask
                YarnChild.main(new String[]{this.jobInfoJson.toJSONString()});
            }


        } else if ("yarn".equals(mapreduce_framework_name)) {

            /**
             * 一、 要请求 resource manager，申请提交作业
             *     resource manager应该给我返回 可用的容器，以及一个application_id
             */
            Socket socket = new Socket("doitedu01", 16020);
            OutputStream out1 = socket.getOutputStream();
            InputStream in1 = socket.getInputStream();
            ObjectOutputStream ot = new ObjectOutputStream(out1);
            ObjectInputStream it = new ObjectInputStream(in1);

            // 准备请求信息 （需要的容器数量）
            ApplicationRequest applicationRequest = new ApplicationRequest(this.mapTaskNumb);

            // 发送请求
            ot.writeObject(applicationRequest);
            ot.flush();

            // 接收响应
            ApplicationResponse resp = (ApplicationResponse) it.readObject();

            if(!resp.getStatus().equals("success")){
                throw new RuntimeException("提交作业失败....");
            }


            // 从响应中提取出 resource manager 所分配的 applicationId和所分配的容器
            String applicationId = resp.getJobId();

            // ["doitedu02:16030","doitedu03:16030"]
            List<String> containerIds = resp.getContainerIds();


            /**
             * 二、根据 resource manager所分配的 application_id 和  容器
             * 去请求 node manager 启动作业进程
             */

            // 获得用户作业的jar包文件
            File jarFile = new File(this.jarPath);
            // 取到jar文件的长度
            int jarLength = (int) jarFile.length();

            // 开启读jar文件的输入流
            FileInputStream fi = new FileInputStream(this.jarPath);

            // 准备接收字节数据的字节数组
            byte[] jarBytes = new byte[jarLength];

            // 将jar文件的内容读到jarBytes中
            fi.read(jarBytes);


            // 给作业参数填充job_id
            this.jobInfoJson.put("job_id", applicationId);
            // 填充要提交的任务阶段
            this.jobInfoJson.put("phase", "map");



            // 准备一个向node manager发送程序启动请求的 context
            AppLaunchContext context = new AppLaunchContext();

            // 设置jar包
            context.setJarBytes(jarBytes);
            context.setMainClass(YarnChild.class.getName());  // top.doe.mr.YarnChild
            context.setApplicationId(applicationId);


            // TODO 获取被分配的node manager
            //String[] nodeManagerList = new String[]{"doitedu02", "doitedu03"};   // 16030

            for (int taskId = 0; taskId < this.mapTaskNumb; taskId++) {
                // 将taskId，放到程序的内部需要的参数中
                this.jobInfoJson.put("task_id",taskId);
                context.setParamJson(this.jobInfoJson.toJSONString());

                // 将taskId，放到node manager所需要的启动上下文中
                context.setTaskId(taskId + "");

                // 轮询给某个NM ： "doitedu02"
                int mod = taskId % (containerIds.size());
                String nodeManageHostAndPort = containerIds.get(mod);
                String[] split = nodeManageHostAndPort.split(":");

                // 向它发送请求
                Socket sc = new Socket(split[0], Integer.parseInt(split[1]));
                InputStream in = sc.getInputStream();
                OutputStream out = sc.getOutputStream();
                ObjectOutputStream oo = new ObjectOutputStream(out);
                ObjectInputStream oi = new ObjectInputStream(in);

                // 发送请求
                oo.writeObject(context);
                oo.flush();

                // 获取NM的响应
                AppLaunchResp o = (AppLaunchResp) oi.readObject();
                if(o.getStatus().equals("fail")){
                    throw new RuntimeException("程序启动请求失败....");
                }

                oo.close();
                oi.close();
                out.close();
                in.close();
                sc.close();

            }


        } else {
            System.out.println("没运行,骗你玩,我不支持");
        }

    }


}
