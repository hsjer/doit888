package top.doe.threads;

import org.apache.flink.util.Collector;
import top.doe.bean.UserEvent;
import top.doe.calculator_model.RuleCalculator;

import java.util.concurrent.CountDownLatch;

/**
 * @Author: 深似海
 * @Site: <a href="www.51doit.com">多易教育</a>
 * @QQ: 657270652
 * @Date: 2024/11/20
 * @Desc: 学大数据，上多易教育
 * 规则calculator的运行器
 **/
public class CalculatorRunner implements Runnable {

    private final RuleCalculator calculator;

    private Iterable<UserEvent> userEvents;

    Collector<String> collector;

    CountDownLatch latch;


    public CalculatorRunner(RuleCalculator calculator) {
        this.calculator = calculator;
    }

    public void setCollector(Collector<String> collector) {
        this.collector = collector;
    }

    public RuleCalculator getCalculator() {
        return calculator;
    }

    public void setUserEvents(Iterable<UserEvent> userEvents) {
        this.userEvents = userEvents;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {
        try {

            for (UserEvent userEvent : userEvents) {
                calculator.calculate(userEvent, collector);
            }

            // 新运算机处理完这些数据后，就不再是新运算机了
            if(calculator.getNewStatus()) calculator.setNewStatus(false);

            latch.countDown();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
