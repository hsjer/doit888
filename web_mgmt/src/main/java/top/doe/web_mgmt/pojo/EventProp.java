package top.doe.web_mgmt.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventProp {
    private String prop_name;
    private String prop_operator;
    private String prop_value;
}
