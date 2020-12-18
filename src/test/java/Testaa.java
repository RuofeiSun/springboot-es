import com.ruofei.ESApplication;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertNull;

/**
 * @Author: srf
 * @Date: 2020/12/9 20:56
 * @description:
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ESApplication.class)
@Slf4j
public class Testaa {

    @Resource
    RestHighLevelClient restHighLevelClient;

    /**
     * 创建索引
     *
     * @exception IOException
     */
    @Test
    public void testCreateIndex() throws IOException
    {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest("srf_test");
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("storeName").field("type", "text").endObject();
        mapping.startObject("desc").field("type", "text").endObject();
        mapping.endObject().endObject();
        createIndexRequest.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 2)
        );
        createIndexRequest.mapping(mapping);
        // 同步创建
        final CreateIndexResponse createIndexResponse = restHighLevelClient.indices()
                .create(createIndexRequest, RequestOptions.DEFAULT);

        // 所有节点是否已确认请求
        if (createIndexResponse.isAcknowledged())
        {
            log.info("创建索引：{}所有节点已确认接收请求!", "index");
        }
        // 是否在超时前为索引中的每个碎片启动了所需数量的分片副本
        if (createIndexResponse.isShardsAcknowledged())
        {
            log.info("创建索引：{}所有节点分片副本都已创建成功!", "index");
        }
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void testDeleteIndex() throws IOException{
        DeleteIndexRequest request = new DeleteIndexRequest("srf_test");
        AcknowledgedResponse deleteIndexResponse = restHighLevelClient.indices().delete(request, RequestOptions.DEFAULT);
    }

    /**
     * 添加文档
     */
    @Test
    public void testAddDoc() throws IOException
    {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("storeName", "pangzhi");
        jsonMap.put("desc", "hello pangzhi");
        IndexRequest indexRequest = new IndexRequest("store_test")
                .source(jsonMap);
        //indexRequest.timeout("10s");
        //indexRequest.version(3);
        indexRequest.versionType(VersionType.INTERNAL);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        System.out.println(indexResponse);
    }

    /**
     * 根据id查整条信息，可以加条件
     * @throws IOException
     */
    @Test
    public void testGetApi() throws IOException
    {
        GetRequest getRequest = new GetRequest(
                "order_test",
                "3");
        String[] includes = new String[]{"ASD00001"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        if (getResponse.isExists()) {
            long version = getResponse.getVersion();
            String sourceAsString = getResponse.getSourceAsString();
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            System.out.println("version："+version);
            System.out.println("sourceAsString:"+sourceAsString);
            System.out.println(sourceAsMap);

            System.out.println(sourceAsBytes.toString());
        } else {

        }
    }

    /**
     * 参数和上面一摸一样，有数据返回true，否则false
     * @throws IOException
     */
    @Test
    public void testExistsApi() throws IOException
    {
        GetRequest getRequest = new GetRequest(
                "order_test",
                "3");
        getRequest.fetchSourceContext(new FetchSourceContext(false));
        getRequest.storedFields("_none_");
        boolean exists = restHighLevelClient.exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 根据id删除
     * @throws IOException
     */
    @Test
    public void testDeleteApi() throws IOException
    {
        DeleteRequest request = new DeleteRequest(
                "order_test",
                "1");
        DeleteResponse deleteResponse = restHighLevelClient.delete(request,RequestOptions.DEFAULT);
        String index = deleteResponse.getIndex();
        String id = deleteResponse.getId();
        long version = deleteResponse.getVersion();
        System.out.println("index:"+index);
        System.out.println("id:"+id);
        System.out.println("version:"+version);
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

        }
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure :
                    shardInfo.getFailures()) {
                String reason = failure.reason();
            }
        }
    }

    /**
     * 更新doc
     * @throws IOException
     */
    @Test
    public void testUpdateApi() throws IOException
    {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("desc", "chuan zhang");
        UpdateRequest request = new UpdateRequest("store_test", "U7MRYXYB1boP2or13O_-")
                .doc(jsonMap);
        //docAsUpsert设为true,如果不存在会新增。
        request.docAsUpsert(true);
        UpdateResponse updateResponse = restHighLevelClient.update(request, RequestOptions.DEFAULT);
        String index = updateResponse.getIndex();
        String id = updateResponse.getId();
        long version = updateResponse.getVersion();
        System.out.println("index:"+index);
        System.out.println("id:"+id);
        System.out.println("version:"+version);
        System.out.println("result:"+updateResponse.getResult() );
    }

    /**
     * 参数和上面一摸一样，有数据返回true，否则false
     * @throws IOException
     */
    @Test
    public void testVectorsApi() throws IOException
    {
        TermVectorsRequest request = new TermVectorsRequest("store_test", "S7NaYHYB1boP2or1s-9N");
        request.setFields("desc");

        request.setTermStatistics(true);
        Map<String, Integer> filterSettings = new HashMap<>();
        filterSettings.put("max_num_terms", 3);
        filterSettings.put("min_term_freq", 1);
        filterSettings.put("max_term_freq", 10);
        filterSettings.put("min_doc_freq", 1);
        filterSettings.put("max_doc_freq", 100);
        filterSettings.put("min_word_length", 1);
        filterSettings.put("max_word_length", 10);

        request.setFilterSettings(filterSettings);

        TermVectorsResponse response =
                restHighLevelClient.termvectors(request, RequestOptions.DEFAULT);
        String index = response.getIndex();
        String id = response.getId();
        boolean found = response.getFound();
        System.out.println("index:"+index);
        System.out.println("id:"+id);
        System.out.println("found:"+found);
        for (TermVectorsResponse.TermVector tv : response.getTermVectorsList()) {
            String fieldname = tv.getFieldName();
            int docCount = tv.getFieldStatistics().getDocCount();
            long sumTotalTermFreq =
                    tv.getFieldStatistics().getSumTotalTermFreq();
            long sumDocFreq = tv.getFieldStatistics().getSumDocFreq();
            System.out.println("filename:"+fieldname+"--docCount:"+docCount+
                    "--sumTotalTermFreq:"+sumTotalTermFreq+
                    "--sumDocFreq:"+sumDocFreq);
            if (tv.getTerms() != null) {
                List<TermVectorsResponse.TermVector.Term> terms =
                        tv.getTerms();
                for (TermVectorsResponse.TermVector.Term term : terms) {
                    String termStr = term.getTerm();
                    int termFreq = term.getTermFreq();
                    int docFreq = term.getDocFreq();
                    long totalTermFreq = term.getTotalTermFreq();
                    float score = term.getScore();
                    System.out.println("-----------------------");
                    System.out.println("termStr:"+termStr);
                    System.out.println("termFreq:"+termFreq);
                    System.out.println("docFreq:"+docFreq);
                    System.out.println("totalTermFreq:"+totalTermFreq);
                    System.out.println("score:"+score);
                    if (term.getTokens() != null) {
                        List<TermVectorsResponse.TermVector.Token> tokens =
                                term.getTokens();
                        for (TermVectorsResponse.TermVector.Token token : tokens) {
                            int position = token.getPosition();
                            int startOffset = token.getStartOffset();
                            int endOffset = token.getEndOffset();
                            String payload = token.getPayload();
                            System.out.println("------------------------");
                            System.out.println("position:"+position);
                            System.out.println("startOffset:"+startOffset);
                            System.out.println("endOffset:"+endOffset);
                            System.out.println("payload:"+payload);
                        }
                    }
                }
            }
        }
    }

    /**
     * 批量，增删改，new 对应的request就可以；返回和之前单个类似
     * @throws IOException
     */
    @Test
    public void testBulkApi() throws IOException
    {
        Map<String, Object> jsonMap1 = new HashMap<>();
        jsonMap1.put("storeName", "wukong");
        jsonMap1.put("desc", "chi an yi bang");
        Map<String, Object> jsonMap2 = new HashMap<>();
        jsonMap2.put("storeName", "bajie");
        jsonMap2.put("desc", "gao lao zhuang");
        BulkRequest request = new BulkRequest("store_test");
        request.add(new IndexRequest()
                .source(jsonMap1));
        request.add(new IndexRequest()
                .source(jsonMap2));
        BulkResponse bulkResponse = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);

        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            switch (bulkItemResponse.getOpType()) {
                case INDEX:
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
            }
        }
    }

    /**
     * 批量get,可以for循环获取结果
     * @throws IOException
     */
    @Test
    public void testMultiGetApi() throws IOException
    {
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item(
                "store_test",
                "UbOmYHYB1boP2or1cu9H"));
        request.add(new MultiGetRequest.Item("store_test", "UrOmYHYB1boP2or1cu9H"));
        MultiGetResponse response = restHighLevelClient.mget(request, RequestOptions.DEFAULT);
        MultiGetItemResponse firstItem = response.getResponses()[0];
        assertNull(firstItem.getFailure());
        GetResponse firstGet = firstItem.getResponse();
        String index = firstItem.getIndex();
        String id = firstItem.getId();
        if (firstGet.isExists()) {
            long version = firstGet.getVersion();
            String sourceAsString = firstGet.getSourceAsString();
            Map<String, Object> sourceAsMap = firstGet.getSourceAsMap();
            byte[] sourceAsBytes = firstGet.getSourceAsBytes();
        } else {

        }
    }

    /**
     * 复制备份index
     * @throws IOException
     */
    @Test
    public void testReindexApi() throws IOException
    {
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("store_test");
        request.setDestIndex("store_copy");
        //optype为create,则只会新增目标index中不存在的doc,存在的会报版本冲突，默认是index
        request.setDestOpType("create");
        //加上setConflicts("proceed")可以避免抛异常，然后计数多少有冲突，
        //request.setConflicts("proceed");
        //可以设置批量提交，一次传多少条，
        //request.setSourceBatchSize(100);
        //备份的条件过滤
        //request.setSourceQuery(new TermQueryBuilder("user", "kimchy"));
        BulkByScrollResponse bulkResponse =
                restHighLevelClient.reindex(request, RequestOptions.DEFAULT);
        //long versionConflicts = bulkResponse.getVersionConflicts();
        //System.out.println(versionConflicts);
    }

    /**
     * 带条件批量update
     * @throws IOException
     */
    @Test
    public void testUpdateByQueryApi() throws IOException
    {
        UpdateByQueryRequest request =
                new UpdateByQueryRequest("store_test");
        request.setQuery(new TermQueryBuilder("storeName", "jay"));
        request.setScript(
                new Script(
                        ScriptType.INLINE, "painless",
                        "if (ctx._source.storeName == 'jay') {ctx._source.desc ='ho ho ho';}",
                        Collections.emptyMap()));
        BulkByScrollResponse bulkResponse =
                restHighLevelClient.updateByQuery(request, RequestOptions.DEFAULT);
    }

    /**
     * 带条件批量delete
     * @throws IOException
     */
    @Test
    public void testDeleteByQueryApi() throws IOException
    {
        DeleteByQueryRequest request =
                new DeleteByQueryRequest("store_test");
        request.setQuery(new TermQueryBuilder("storeName", "lin"));

        BulkByScrollResponse bulkResponse =
                restHighLevelClient.deleteByQuery(request, RequestOptions.DEFAULT);
    }


}
