package top.doe.web_mgmt.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventOpt {
    private int id;
    private String name;
    private List<String> props;
}
