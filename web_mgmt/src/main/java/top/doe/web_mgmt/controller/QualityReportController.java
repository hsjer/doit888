package top.doe.web_mgmt.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import top.doe.web_mgmt.service.QualityReportService;

import java.security.acl.LastOwnerException;
import java.sql.SQLException;
import java.util.List;

@RestController
public class QualityReportController {


    /**
     * {
     *     "table1Data": [
     *         ["2024-01-01", "server1", 5, 1200],
     *         ["2024-01-02", "server2", 6, 1500]
     *     ],
     *     "table2Data": [
     *         ["2024-01-01", 12, 3000]
     *     ],
     *     "table3Data": [
     *         ["2024-01-01", 5000, 4500]
     *     ]
     * }
     */


    @Autowired
    QualityReportService qualityReportService;

    @RequestMapping("/api/get-log-report")
    public JSONObject getReport(String type,String date) throws SQLException {

        JSONObject jsonObj = new JSONObject();


        List<JSONArray> logServerDatas = qualityReportService.queryApplogServerReport(date);
        JSONArray arr1 = new JSONArray(logServerDatas);
        jsonObj.put("table1Data",arr1);


        JSONArray hdfsDatas = qualityReportService.queryApplogHdfsReport(date);
        JSONArray arr2 = new JSONArray();
        arr2.add(hdfsDatas);
        jsonObj.put("table2Data",arr2);


        // 去重后的ods层的dt日期的总行数
        int lines_amt = qualityReportService.queryApplogOdsReport(date);
        // 日志服务器端的 dt日期的总行数
        int sum = 0;
        for (JSONArray logServerData : logServerDatas) {
            int server_lines = logServerData.getIntValue(3);
            sum += server_lines;
        }

        JSONArray arrInner3 = new JSONArray();
        arrInner3.add(date);
        arrInner3.add(sum);
        arrInner3.add(lines_amt);

        JSONArray arr3 = new JSONArray();
        arr3.add(arrInner3);
        jsonObj.put("table3Data",arr3);


        // 判断是否有数据丢失

        jsonObj.put("healthy",lines_amt<sum?0:1);

        System.out.println(jsonObj.toJSONString());


        return jsonObj;

    }

}
