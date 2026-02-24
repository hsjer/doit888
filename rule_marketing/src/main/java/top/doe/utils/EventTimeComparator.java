package top.doe.utils;

import top.doe.bean.UserEvent;

import java.io.Serializable;
import java.util.Comparator;

public class EventTimeComparator implements Comparator<UserEvent> , Serializable {
    @Override
    public int compare(UserEvent o1, UserEvent o2) {
        return Long.compare(o1.getAction_time(),o2.getAction_time());
    }
}
