package top.doe.web_mgmt.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RuleInfo {
    int id;
    String rule_name;
    int model_id;
    int crowd_cnt;
    String param_json;
    int rule_status;
    long hist_end;
    String creator;
    String auditor;
}
