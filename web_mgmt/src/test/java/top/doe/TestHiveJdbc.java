package top.doe;

import java.sql.*;

public class TestHiveJdbc {
    public static void main(String[] args) throws SQLException {


        Connection conn = DriverManager.getConnection("jdbc:hive2://doitedu01:10000", "root", "");
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select * from ods.user_action_log");

        while (resultSet.next()){
            String username = resultSet.getString("username");
            String device_type = resultSet.getString("device_type");

            System.out.println(username + "\t"+device_type);
        }


        resultSet.close();
        stmt.close();
        conn.close();


    }
}
