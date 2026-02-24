package top.doe.web_mgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class LogServerApplog {

    private String log_server;
    private String file_name;
    private int line_cnt;
    private String target_dt;
    private Timestamp create_time;

}
