package top.doe.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent implements Comparable<UserEvent>,Serializable {

    private long user_id;
    private String account;
    private String event_id;
    private Map<String,String> properties;
    private long action_time;


    @Override
    public int compareTo(UserEvent o) {
        return Long.compare(this.action_time,o.action_time);
    }
}
