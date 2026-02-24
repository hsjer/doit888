package top.doe.web_mgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class OdsApplog {

    private String database;
    private String table;
    private String partition;
    private int lines_amt = -1;
    private Timestamp create_time;

}
