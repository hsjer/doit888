package top.doe.web_mgmt.service;

import com.alibaba.fastjson.JSONArray;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


@Service
public class QualityReportServiceImpl implements QualityReportService {

    Connection connection;
    public QualityReportServiceImpl() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50","root","ABC123.abc123");
    }


    @Override
    public List<JSONArray> queryApplogServerReport(String dt) throws SQLException {

        // ["2024-01-01", "server1", 5, 1200]
        PreparedStatement preparedStatement = connection.prepareStatement("select target_dt,log_server,count(1) as file_cnt, sum(line_cnt) as line_amt from dq_applog_detail where target_dt = ? group by target_dt,log_server");
        preparedStatement.setString(1,dt);

        ResultSet resultSet = preparedStatement.executeQuery();

        ArrayList<JSONArray> lst = new ArrayList<>();
        while(resultSet.next()){
            String targetDt = resultSet.getString("target_dt");
            String logServer = resultSet.getString("log_server");
            long file_cnt = resultSet.getLong("file_cnt");
            long line_amt = resultSet.getLong("line_amt");

            JSONArray jsonArray = new JSONArray();
            jsonArray.add(targetDt);
            jsonArray.add(logServer);
            jsonArray.add(file_cnt);
            jsonArray.add(line_amt);

            lst.add(jsonArray);
        }

        return lst;
    }

    @Override
    public JSONArray queryApplogHdfsReport(String dt) throws SQLException {

        //  ["2024-01-01", 12, 3000]
        PreparedStatement preparedStatement = connection.prepareStatement("select file_cnt,lines_amt from dq_applog_hdfs where target_dt = ?");
        preparedStatement.setString(1,dt);

        ResultSet resultSet = preparedStatement.executeQuery();

        resultSet.next();

        int file_cnt = resultSet.getInt("file_cnt");
        int lines_amt = resultSet.getInt("lines_amt");

        JSONArray jsonArray = new JSONArray();
        jsonArray.add(dt);
        jsonArray.add(file_cnt);
        jsonArray.add(lines_amt);

        return jsonArray;
    }

    @Override
    public int queryApplogOdsReport(String dt) throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement("select lines_amt from dq_applog_ods where partition_name = ?");
        preparedStatement.setString(1,dt);

        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        int linesAmt = resultSet.getInt("lines_amt");

        return linesAmt;
    }
}
