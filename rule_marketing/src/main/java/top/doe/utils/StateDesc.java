package top.doe.utils;

import org.apache.flink.api.common.state.MapStateDescriptor;
import top.doe.bean.RuleMeta;

public class StateDesc {

    public static final MapStateDescriptor<Integer, RuleMeta> BC_STATE_DESC = new MapStateDescriptor<>("rule_metas", Integer.class, RuleMeta.class);
}
