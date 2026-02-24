package top.doe.web_mgmt.service;

import top.doe.web_mgmt.pojo.HdfsApplog;
import top.doe.web_mgmt.pojo.LogServerApplog;
import top.doe.web_mgmt.pojo.OdsApplog;

import java.sql.SQLException;

public interface DqApplogService {
    boolean saveLogServerApplogInfo(LogServerApplog applog) throws SQLException;
    boolean saveHdfsApplogInfo(HdfsApplog hdfsApplog) throws SQLException;

    boolean saveOdsApplogInfo(OdsApplog applog) throws SQLException;
}
