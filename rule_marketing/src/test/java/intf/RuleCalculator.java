package intf;

import com.alibaba.fastjson.JSONObject;

public interface RuleCalculator {

    public void init(String param, JSONObject jsonObject);

    public void calc(String event);

}
