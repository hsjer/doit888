package top.doe.web_mgmt.service;

import org.springframework.stereotype.Service;
import top.doe.web_mgmt.pojo.HdfsApplog;
import top.doe.web_mgmt.pojo.LogServerApplog;
import top.doe.web_mgmt.pojo.OdsApplog;

import java.sql.*;


@Service
public class DqApplogServiceImpl implements DqApplogService {

    Connection connection;
    public DqApplogServiceImpl() throws SQLException {
        //connection = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50","root","ABC123.abc123");
    }

    @Override
    public boolean saveLogServerApplogInfo(LogServerApplog applog) throws SQLException {

        PreparedStatement preparedStatement = connection.prepareStatement("insert into dq_applog_detail (log_server,file_name,line_cnt,target_dt,create_time) values(?,?,?,?,?)");
        preparedStatement.setString(1, applog.getLog_server());
        preparedStatement.setString(2, applog.getFile_name());
        preparedStatement.setInt(3,applog.getLine_cnt());
        preparedStatement.setString(4,applog.getTarget_dt());
        preparedStatement.setTimestamp(5,applog.getCreate_time());

        boolean execute = preparedStatement.execute();

        return execute;
    }

    @Override
    public boolean saveHdfsApplogInfo(HdfsApplog hdfsApplog) throws SQLException {


        PreparedStatement preparedStatement = connection.prepareStatement("insert into dq_applog_hdfs (target_dt,file_cnt,lines_amt,create_time) values(?,?,?,?)");
        preparedStatement.setString(1, hdfsApplog.getTarget_dt());
        preparedStatement.setInt(2,hdfsApplog.getFile_cnt());
        preparedStatement.setInt(3,hdfsApplog.getLines_amt());
        preparedStatement.setTimestamp(4,hdfsApplog.getCreate_time());

        boolean execute = preparedStatement.execute();

        return execute;
    }

    @Override
    public boolean saveOdsApplogInfo(OdsApplog applog) throws SQLException {
        PreparedStatement preparedStatement = connection.prepareStatement("insert into dq_applog_ods (db_name,table_name,partition_name,lines_amt,create_time) values(?,?,?,?,?)");
        preparedStatement.setString(1, applog.getDatabase());
        preparedStatement.setString(2,applog.getTable());
        preparedStatement.setString(3,applog.getPartition());
        preparedStatement.setInt(4,applog.getLines_amt());
        preparedStatement.setTimestamp(5,applog.getCreate_time());

        boolean execute = preparedStatement.execute();

        return execute;
    }
}
