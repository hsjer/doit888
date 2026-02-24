package top.doe.web_mgmt.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import top.doe.web_mgmt.pojo.HdfsApplog;
import top.doe.web_mgmt.pojo.LogServerApplog;
import top.doe.web_mgmt.pojo.OdsApplog;
import top.doe.web_mgmt.service.DqApplogService;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

@RestController
public class DqApplogController {

    @Autowired
    DqApplogService dqApplogService;


    /**
     * 接口文档：
     *   服务器： 192.168.77.2
     *   端口: 8080
     *   api路径: /api/dp/logserver/applog
     *   请求方式: POST
     *   参数：
     *      {
     *         "log_server":"doitedu01",
     *         "file_name":"/opt/data/app_log/log_2024-10-28_app.log.2",
     *         "line_cnt": 150000,
     *         "target_dt":"2024-10-28"
     *      }
     */
    @RequestMapping(value = "/api/dp/logserver/applog",method = RequestMethod.POST)
    public int reportLogServerInfo(@RequestBody LogServerApplog applog) throws SQLException {

        if(applog == null || StringUtils.isEmpty(applog.getLog_server())){
            return 1;
        }else if(StringUtils.isEmpty(applog.getFile_name())){
            return 2;
        }

        applog.setCreate_time(Timestamp.valueOf(LocalDateTime.now()));

        dqApplogService.saveLogServerApplogInfo(applog);

        return 0;
    }


    @RequestMapping(value = "/api/dp/hdfs/applog",method = RequestMethod.POST)
    public int reportHdfsApplogInfo(@RequestBody HdfsApplog hdfsApplog) throws SQLException {
        if(hdfsApplog == null || StringUtils.isEmpty(hdfsApplog.getTarget_dt())){
            return 1;
        }else if(hdfsApplog.getLines_amt() == -1){
            return 2;
        }


        hdfsApplog.setCreate_time(Timestamp.valueOf(LocalDateTime.now()));

        dqApplogService.saveHdfsApplogInfo(hdfsApplog);

        return 0;
    }


    @RequestMapping(value = "/api/dp/ods/applog",method = RequestMethod.POST)
    public int reportOdsApplogInfo(@RequestBody OdsApplog applog) throws SQLException {
        if(applog == null || StringUtils.isEmpty(applog.getTable())){
            return 1;
        }else if(StringUtils.isEmpty(applog.getPartition())){
            return 2;
        }

        applog.setCreate_time(Timestamp.valueOf(LocalDateTime.now()));

        dqApplogService.saveOdsApplogInfo(applog);

        return 0;
    }


}
