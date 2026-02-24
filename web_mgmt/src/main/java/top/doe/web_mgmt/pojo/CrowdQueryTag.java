package top.doe.web_mgmt.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CrowdQueryTag {
    private String tag_name;
    private String tag_value;
    private String tag_operator;

}
