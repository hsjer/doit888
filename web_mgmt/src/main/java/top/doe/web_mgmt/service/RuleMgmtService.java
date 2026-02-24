package top.doe.web_mgmt.service;

import org.springframework.stereotype.Service;
import top.doe.web_mgmt.pojo.EventOpt;
import top.doe.web_mgmt.utils.ConnectionFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Service
public class RuleMgmtService {
    Connection conn;
    public List<EventOpt> getEventOptions() throws SQLException {

        if (conn == null) {
            conn = ConnectionFactory.getRuleMarketingMySqlConnection();
        }


        List<EventOpt> eventOpts = new ArrayList<EventOpt>();

        PreparedStatement pst = conn.prepareStatement("select id,event_name,props from dw_50.dict_events");
        ResultSet rs = pst.executeQuery();
        while (rs.next()) {

            EventOpt eventOpt = new EventOpt();
            eventOpt.setId(rs.getInt("id"));
            eventOpt.setName(rs.getString("event_name"));
            String propsStr = rs.getString("props");
            String[] props = propsStr.split(",");
            eventOpt.setProps(Arrays.asList(props));

            eventOpts.add(eventOpt);

        }


        return eventOpts;
    }


}
