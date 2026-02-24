package top.doe.web_mgmt.controller;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import top.doe.web_mgmt.pojo.*;
import top.doe.web_mgmt.service.RuleMgmtService;
import top.doe.web_mgmt.utils.ConnectionFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

//@RestController
public class RuleMgmtController {

    //@Autowired
    RuleMgmtService ruleMgmtService;

    @RequestMapping("/api/get-tags")
    public TagOptsVo getTags() throws SQLException {
        System.out.println("请求tag下拉选项..............");
//        List<String> list = new ArrayList<String>();
//        list.add("年龄[10,60]");
//        list.add("性别[male|female]");
//        list.add("活跃度[ABC]");

        List<TagOpt> list = new ArrayList<TagOpt>();
        Connection conn = ConnectionFactory.getRuleMarketingMySqlConnection();
        ResultSet resultSet = conn.prepareStatement("select * from dw_50.dict_tag").executeQuery();
        while (resultSet.next()) {

            String tag_name = resultSet.getString("tag_name");
            String tag_alias = resultSet.getString("tag_alias");
            String value_desc = resultSet.getString("value_desc");
            String value_type = resultSet.getString("value_type");

            list.add(new TagOpt(tag_name,tag_alias,value_desc,value_type));
        }


        return new TagOptsVo(list);
    }


    @RequestMapping("/api/crowd-selection")
    public CrowdQueryResVo queryCrowd(@RequestBody CrowdQueryParam queryParam) throws IOException {
        List<CrowdQueryTag> queryConditions = queryParam.getQuery_conditions();

        RestHighLevelClient esClient = ConnectionFactory.getEsClient();

        for (CrowdQueryTag queryCondition : queryConditions) {
            String tagName = queryCondition.getTag_name();
            String tagValue = queryCondition.getTag_value();
            System.out.println("预选条件: " + tagName +","+tagValue);
        }

        MatchPhraseQueryBuilder matchQueryBuilder = QueryBuilders.matchPhraseQuery("tag_03_01", "空调");

        // 范围查询条件
        RangeQueryBuilder ageRangeQuery = QueryBuilders.rangeQuery("tag_01_04").lte(80).gt(25);


        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(matchQueryBuilder).must(ageRangeQuery);

        SearchRequest request = new SearchRequest("doit50_profile");
        request.source(new SearchSourceBuilder().query(boolQueryBuilder));
        SearchResponse response = esClient.search(request, RequestOptions.DEFAULT);

        long value = response.getHits().getTotalHits().value;

        esClient.close();

        return new CrowdQueryResVo((int) value);
    }



    @RequestMapping("/api/get-events")
    public OptionEventsVo queryEventOpts() throws SQLException {
        //List<EventVo> list = new ArrayList<>();
        //list.add(new EventVo(1,"添加购物车", Arrays.asList("item_id","page_url")));
        //list.add(new EventVo(2,"收藏音乐" , Arrays.asList("music_id","page_url")) );
        //list.add(new EventVo(3,"文章点赞" , Arrays.asList("article_id","page_url")) );
        //list.add(new EventVo(4,"文章分享" , Arrays.asList("article_id","page_url")) );

        List<EventOpt> eventOptions = ruleMgmtService.getEventOptions();

        return  new OptionEventsVo(eventOptions);
    }



    //
    @RequestMapping("/api/submit-rule")
    public int submitRule(@RequestBody M11Param m11Param){
        System.out.println(m11Param);

        return  1;
    }

}
