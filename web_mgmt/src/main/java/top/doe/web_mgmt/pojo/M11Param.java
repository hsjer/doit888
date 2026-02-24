package top.doe.web_mgmt.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class M11Param {
    private int event;
    private String startTime;
    private String endTime;
    private List<CrowdQueryTag> crowdQueryTags;
    private List<EventProp>eventProps;
}
