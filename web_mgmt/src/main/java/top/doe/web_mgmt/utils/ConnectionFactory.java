package top.doe.web_mgmt.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {

    private static RestHighLevelClient client;
    private static Connection connection2;
    private static Connection connection1;
    private static Connection connection3;

    public static RestHighLevelClient getEsClient() {

        if(client == null) {
            client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu01", 9200, "http")));
        }
        return client;
    }

    public static Connection getHiveConnection() throws SQLException {

        if(connection1 == null) {
            connection1 = DriverManager.getConnection("jdbc:hive2://doitedu01:10000", "root", "");
        }
        return connection1;
    }


    public static Connection getDqMySqlConnection() throws SQLException {

        if(connection2 == null) {
            connection2 = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50", "root", "ABC123.abc123");
        }
        return connection2;
    }


    public static Connection getRuleMarketingMySqlConnection() throws SQLException {

        if(connection3 == null) {
            connection3 = DriverManager.getConnection("jdbc:mysql://doitedu01:3306/dw_50", "root", "ABC123.abc123");
        }
        return connection3;
    }

}
