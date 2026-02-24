package top.doe.calculator_model;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.util.Collector;
import top.doe.bean.RuleMeta;
import top.doe.bean.UserEvent;

public interface RuleCalculator {


    public boolean isNew = true;

    public boolean getNewStatus();

    public void setNewStatus(boolean newStatus);

    public int getRuleId();


    /**
     * 规则运算机实例的初始化方法
     * @param runtimeContext  flink的runtimeContext
     * @param ruleMeta   规则的元数据对象
     */
    public void initialize(RuntimeContext runtimeContext, RuleMeta ruleMeta, KeyedStateStore keyedStateStore) throws Exception;


    /**
     * 规则运算机实例处理数据的方法
     * @param userEvent  输入的用户事件
     * @param collector  flink的输出器
     */
    public void calculate(UserEvent userEvent,Collector<String> collector) throws Exception;


    public void destroy(AbstractStreamOperator<?> operator);


}
