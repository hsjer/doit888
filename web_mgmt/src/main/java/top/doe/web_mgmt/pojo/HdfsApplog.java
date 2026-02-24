package top.doe.web_mgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class HdfsApplog {

    private int file_cnt;
    private int lines_amt = -1;
    private String target_dt;
    private Timestamp create_time;

}
