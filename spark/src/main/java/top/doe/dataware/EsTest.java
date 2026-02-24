package top.doe.dataware;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

public class EsTest {
    public static void main(String[] args) throws IOException {

        // es的请求客户端
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("doitedu01", 9200, "http")));

        // 用于查询参数封装的对象
        SearchRequest request = new SearchRequest("doit50_profile");


        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("tag_03_01", "省电");


        request.source(new SearchSourceBuilder().query(matchQueryBuilder));


        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }

        client.close();

    }
}
