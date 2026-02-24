package top.doe.web_mgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class CrowdQueryParam {

    private String query_time;
    private List<CrowdQueryTag> query_conditions;


}
