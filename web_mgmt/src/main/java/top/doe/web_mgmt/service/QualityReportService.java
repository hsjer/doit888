package top.doe.web_mgmt.service;

import com.alibaba.fastjson.JSONArray;

import java.sql.SQLException;
import java.util.List;

public interface QualityReportService {
    // 查询日志服务器端的日志行数统计报表
    List<JSONArray> queryApplogServerReport(String dt) throws SQLException;

    // 查询hdfs端落地日志行数统计报表
    JSONArray queryApplogHdfsReport(String dt) throws SQLException;

    // 查询去重后日志行数统计报表
    int queryApplogOdsReport(String dt) throws SQLException;

}
