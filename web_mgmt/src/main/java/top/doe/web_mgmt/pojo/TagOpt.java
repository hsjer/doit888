package top.doe.web_mgmt.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TagOpt {
    private String tag_name;
    private String tag_alias;
    private String value_desc;
    private String value_type;
}
