package top.doe.web_mgmt.controller;

import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import top.doe.web_mgmt.pojo.RuleVo;
import top.doe.web_mgmt.utils.ConnectionFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RestController
public class DqCheckRuleController {

    HashMap<String, String> ruleDict;
    Connection conn;
    Statement stmt;

    public DqCheckRuleController() throws SQLException {
        // 0: 空值检查
        // 1: 最大值
        // 2: 最小值
        // 3: sum
        ruleDict = new HashMap<>();
        ruleDict.put("0","select count(1) as check_res_value from $_TABLE where $_FIELD is null");
        ruleDict.put("1","select max($_FIELD) as check_res_value from $_TABLE");


        //Connection conn = DriverManager.getConnection("jdbc:hive2://doitedu01:10000", "root", "");
        //stmt = conn.createStatement();

    }


    @RequestMapping(value = "/api/dq/check",method = RequestMethod.POST)
    public boolean submitCheckRule(@RequestBody RuleVo ruleVo) throws SQLException {

        if(conn == null ) {
            conn = ConnectionFactory.getHiveConnection();
            stmt = conn.createStatement();
        }


        // RuleVo(ruleType=2, targetTable=ods.xxx, targetField=yyy)
        String sqlTemplate = ruleDict.get(ruleVo.getRuleType());
        String $_table = sqlTemplate.replace("$_TABLE", ruleVo.getTargetTable());
        String sql = $_table.replace("$_FIELD", ruleVo.getTargetField());

        // 把生成好的sql，发给hive去执行
        ResultSet resultSet = stmt.executeQuery(sql);


        // 获取执行的结果
        Long checkResValue = null;
        while(resultSet.next()){
            checkResValue = resultSet.getLong("check_res_value");
        }

        // 简化：直接打印检查结果
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("rule_type",ruleVo.getRuleType());
        jsonObject.put("target_table",ruleVo.getTargetTable());
        jsonObject.put("target_field",ruleVo.getTargetField());
        jsonObject.put("null_cnt",checkResValue);


        System.out.println(jsonObject.toJSONString());

        return true;
    }


}
