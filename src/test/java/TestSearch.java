import com.ruofei.ESApplication;
import com.ruofei.service.doc.DocService;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @Author: srf
 * @Date: 2020/12/15 11:32
 * @description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ESApplication.class)
@Slf4j
public class TestSearch {


    @Resource
    RestHighLevelClient restHighLevelClient;

    @Resource
    DocService docService;

    @Test
    public void testYYY(){
        List<String> ikList = docService.getIkList("hello eason", "store_test", "storeName");
        System.out.println(ikList);
    }


    @Test
    public void testSearch() throws IOException
    {
        SearchRequest searchRequest = new SearchRequest("store_test");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle =
                new HighlightBuilder.Field("desc");
        highlightTitle.highlighterType("unified");
        highlightBuilder.field(highlightTitle);
        searchSourceBuilder.highlighter(highlightBuilder);

        //match进行搜索的时候，会先进行分词拆分，拆完后，再来匹配
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("desc", "hello eason");
        //matchPhrase查询时必须包含完整字段。不会分词拆分
        //MatchPhraseQueryBuilder matchQueryBuilder = new MatchPhraseQueryBuilder("desc", "hello eason");

        //term属于精确匹配，只能查单个词；我想用term匹配多个词怎么做？可以使用terms来
        //TermsQueryBuilder termQueryBuilder = QueryBuilders.termsQuery("desc",new String[]{"hello","eason"});
        searchSourceBuilder.query(matchQueryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        RestStatus status = searchResponse.status();
        TimeValue took = searchResponse.getTook();
        Boolean terminatedEarly = searchResponse.isTerminatedEarly();
        boolean timedOut = searchResponse.isTimedOut();

        System.out.println("status:"+status);
        System.out.println("took:"+took);
        System.out.println("terminatedEarly:"+terminatedEarly);
        System.out.println("timedOut:"+timedOut);

        int totalShards = searchResponse.getTotalShards();
        int successfulShards = searchResponse.getSuccessfulShards();
        int failedShards = searchResponse.getFailedShards();
        for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
            // failures should be handled here
        }
        System.out.println("----------------------------------");
        System.out.println("totalShards:"+totalShards);
        System.out.println("successfulShards:"+successfulShards);
        System.out.println("failedShards:"+failedShards);

        System.out.println("----------------------------------");
        SearchHits hits = searchResponse.getHits();
        TotalHits totalHits = hits.getTotalHits();
        // the total number of hits, must be interpreted in the context of totalHits.relation
        long numHits = totalHits.value;
        // whether the number of hits is accurate (EQUAL_TO) or a lower bound of the total (GREATER_THAN_OR_EQUAL_TO)
        TotalHits.Relation relation = totalHits.relation;
        float maxScore = hits.getMaxScore();
        System.out.println("numHits:"+numHits);
        System.out.println("relation:"+relation);
        System.out.println("maxScore:"+maxScore);
        System.out.println("======================================");
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            String index = hit.getIndex();
            String id = hit.getId();
            float score = hit.getScore();
            String sourceAsString = hit.getSourceAsString();
            System.out.println("index:"+index);
            System.out.println("id:"+id);
            System.out.println("score:"+score);
            System.out.println("sourceAsString:"+sourceAsString);
            System.out.println("************************************");
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlight = highlightFields.get("desc");
            Text[] fragments = highlight.fragments();
            String fragmentString = fragments[0].string();
            System.out.println("fragmentString:"+fragmentString);
        }
    }

}
