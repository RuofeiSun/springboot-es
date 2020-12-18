package com.ruofei.service.doc.impl;

import com.alibaba.fastjson.JSONObject;
import com.ruofei.constants.ElasticsearchOperationEnum;
import com.ruofei.dto.DocumentDTO;
import com.ruofei.exception.SearchExceptionUtil;
import com.ruofei.service.doc.DocService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.rest.RestStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Author: srf
 * @Date: 2020/12/17 10:48
 * @description:
 */
@Slf4j
@Service
public class DocServiceImpl implements DocService {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Override
    public boolean addDoc(DocumentDTO doc, boolean async) {

        if (null == doc || StringUtils.isBlank(doc.getIndex()) || CollectionUtils.isEmpty(doc.getJsonMap()))
        {
            log.error("添加文档失败，参数必须索引，json格式文档内容！");
            return false;
        }
        String index = doc.getIndex().toLowerCase();
        String id = doc.getId();
        Map<String, Object> jsonMap = doc.getJsonMap();
        String routing = doc.getRouting();

        IndexRequest indexRequest = new IndexRequest(index);
        //id为空的话，es会生成自己的UUID
        if (StringUtils.isNotBlank(id))
        {
            indexRequest.id(id);
        }
        if (StringUtils.isNotBlank(routing))
        {
            indexRequest.routing(routing);
        }

        indexRequest.source(jsonMap).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        if (async)
        {
            final String tempIndex = index;
            final Map<String, Object> tempDoc = jsonMap;
            restHighLevelClient.indexAsync(indexRequest, RequestOptions.DEFAULT, new ActionListener<IndexResponse>()
            {
                @Override
                public void onResponse(IndexResponse indexResponse)
                {
                    // 创建或更新成功
                    if (indexResponse.status() != RestStatus.OK)
                    {
                        log.error("往索引：{}添加文档：{}失败，返回的状态码：{}，返回的结果：{}", tempIndex, tempDoc, indexResponse
                                .status(), indexResponse.getResult());
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("索引：{}添加文档：{}发生异常！", tempIndex, tempDoc);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
                }
            });
            return true;
        }
        else
        {
            boolean result = false;
            try
            {
                final IndexResponse response = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
                // 创建或更新成功
                if (response.status() == RestStatus.OK || response.getResult() == DocWriteResponse.Result.CREATED
                        || response.getResult() == DocWriteResponse.Result.UPDATED)
                {
                    result = true;
                }
                else
                {
                    log.error("往索引：{}添加文档：{}失败，返回的状态码：{}，返回的结果：{}", index, jsonMap, response.status(), response
                            .getResult());
                }
            }
            catch (Exception e)
            {
                log.error("索引：{}添加文档：{}发生异常！", index, jsonMap);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.ADD_DOCUMENT);
            }
            return result;
        }

    }

    @Override
    public boolean addJsonDoc(List<DocumentDTO> docDataList, boolean async) {

        if (CollectionUtils.isEmpty(docDataList))
        {
            log.error("批量添加文档失败，要添加的文档数量为空!");
            return false;
        }

        boolean flag = false;
        BulkRequest bulkRequest = new BulkRequest();
        docDataList.forEach(docData ->
        {
            IndexRequest indexRequest = new IndexRequest(docData.getIndex().toLowerCase());
            if (StringUtils.isNotBlank(docData.getId()))
            {
                indexRequest.id(docData.getId());
            }
            if (StringUtils.isNotBlank(docData.getRouting()))
            {
                indexRequest.routing(docData.getRouting());
            }

            indexRequest.source(docData.getJsonMap());
            bulkRequest.add(indexRequest).setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        });

        if (async)
        {
            // 异步
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>()
            {
                @Override
                public void onResponse(BulkResponse bulkItemResponses)
                {
                    if (bulkItemResponses.hasFailures())
                    {
                        log.warn("批量添加索引文档有失败数据，可能失败原因：{}", bulkItemResponses.buildFailureMessage());
                        bulkItemResponses.forEach(bulkItemResponse ->
                        {
                            if (bulkItemResponse.isFailed())
                            {
                                log.error("索引：{}下添加文档ID：{}的文档失败，失败原因：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                        .getId(), bulkItemResponse.getFailureMessage());
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
                }
            });
            flag = true;
        }
        else
        {
            // 同步
            try
            {
                final BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulk.forEach(bulkItemResponse ->
                {
                    if (bulkItemResponse.isFailed())
                    {
                        log.error("索引：{}下添加文档ID：{}的文档失败，失败原因：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                .getId(), bulkItemResponse.getFailureMessage());
                    }
                });
                flag = true;
            }
            catch (Exception e)
            {
                log.error("批量添加文档发生了异常!");
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
            }
        }
        return flag;
    }

    @Override
    public boolean updateDoc(DocumentDTO updateDoc, boolean async) {
        if (Objects.isNull(updateDoc) || StringUtils.isBlank(updateDoc.getId()) || StringUtils
                .isBlank(updateDoc.getIndex()) || CollectionUtils.isEmpty(updateDoc.getJsonMap()))
        {
            log.error("文档更新失败，更新的文档参数不能为空，请求参数数据：{}", Objects.isNull(updateDoc) ?
                    "null" :
                    JSONObject.toJSONString(updateDoc));
            return false;
        }
        boolean updateFlag = false;
        UpdateRequest updateRequest = new UpdateRequest(updateDoc.getIndex().toLowerCase(), updateDoc.getId());
        updateRequest.doc(updateDoc.getJsonMap());

        String routing = updateDoc.getRouting();
        if (StringUtils.isNotBlank(routing))
        {
            updateRequest.routing(routing);
        }

        if (async)
        {
            updateFlag = true;
            restHighLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, new ActionListener<UpdateResponse>()
            {
                @Override
                public void onResponse(UpdateResponse updateResponse)
                {
                    if (updateResponse.getResult() != DocWriteResponse.Result.UPDATED)
                    {
                        log.error("文档更新出现异常，待更新的原始数据：{}", JSONObject.toJSONString(updateDoc));
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("文档更新出现异常，待更新的原始数据：{}", JSONObject.toJSONString(updateDoc));
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.UPDATE_DOCUMENT);
                }
            });
        }
        else
        {
            try
            {
                final UpdateResponse update = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
                updateFlag = update.getResult() == DocWriteResponse.Result.UPDATED;
            }
            catch (Exception e)
            {
                log.error("文档更新出现异常，待更新的原始数据：{}", JSONObject.toJSONString(updateDoc));
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.UPDATE_DOCUMENT);
            }
        }

        return updateFlag;
    }

    @Override
    public boolean batchUpdateDoc(List<DocumentDTO> updateDocList, boolean async) {
        if (CollectionUtils.isEmpty(updateDocList))
        {
            log.error("批量更新文档失败，请求参数不能为空！");
            return false;
        }

        boolean updateFlag = false;
        BulkRequest bulkRequest = new BulkRequest();
        updateDocList.forEach(elasticsearchDocDo ->
        {
            final String id = elasticsearchDocDo.getId();
            final String index = elasticsearchDocDo.getIndex();
            final Map<String, Object> jsonMap = elasticsearchDocDo.getJsonMap();
            final String routing = elasticsearchDocDo.getRouting();
            if (StringUtils.isBlank(id) || StringUtils.isBlank(index) || CollectionUtils.isEmpty(jsonMap))
            {
                log.error("文档更新失败，更新的文档参数不能为空，请求参数数据：{}", JSONObject.toJSONString(elasticsearchDocDo));
                return;
            }

            UpdateRequest doc = new UpdateRequest(index.toLowerCase(), id).doc(jsonMap);
            if (StringUtils.isNotEmpty(routing))
            {
                doc.routing(routing);
            }
            bulkRequest.add(doc);
        });

        if (async)
        {
            // 异步
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>()
            {
                @Override
                public void onResponse(BulkResponse bulkItemResponses)
                {
                    if (bulkItemResponses.hasFailures())
                    {
                        log.warn("批量更新文档有失败数据，可能失败原因：{}", bulkItemResponses.buildFailureMessage());
                        bulkItemResponses.forEach(bulkItemResponse ->
                        {
                            if (bulkItemResponse.isFailed())
                            {
                                log.error("索引：{}下更新文档ID：{}的文档失败，失败原因：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                        .getId(), bulkItemResponse.getFailureMessage());
                            }
                        });
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
                }
            });
            updateFlag = true;
        }
        else
        {
            // 同步
            try
            {
                final BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulk.forEach(bulkItemResponse ->
                {
                    if (bulkItemResponse.isFailed())
                    {
                        log.error("更新文档发生了失败，当前带更新的文档索引：{}，文档ID：{}，失败原因：{}", bulkItemResponse
                                .getIndex(), bulkItemResponse.getId(), bulkItemResponse.getFailureMessage());
                    }
                });
                updateFlag = true;
            }
            catch (Exception e)
            {
                log.error("更新文档发生了异常!");
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_ADD_DOCUMENT);
            }
        }
        return updateFlag;
    }

    @Override
    public boolean deleteById(String id, String index, boolean async) {
        if (StringUtils.isBlank(index) || StringUtils.isBlank(id))
        {
            log.error("删除文档失败，请求参数id:{},index:{}为空", id, index);
            return false;
        }
        boolean deleteFlag = false;
        DeleteRequest request = new DeleteRequest(index, id);
        if (async)
        {
            restHighLevelClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>()
            {
                @Override
                public void onResponse(DeleteResponse deleteResponse)
                {
                    if (deleteResponse.status() != RestStatus.OK)
                    {
                        log.error("删除文档出现错误，索引：{}，文档ID：{}，返回状态吗：{}", index, id, deleteResponse.status().getStatus());
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
                }
            });
            deleteFlag = true;
        }
        else
        {
            try
            {
                DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
                deleteFlag = deleteResponse.status() == RestStatus.OK;
            }
            catch (Exception e)
            {
                log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
            }
        }
        return deleteFlag;
    }

    @Override
    public boolean deleteById(DocumentDTO deleteDoc, boolean async) {
        if (Objects.isNull(deleteDoc) || StringUtils.isBlank(deleteDoc.getId()) || StringUtils
                .isBlank(deleteDoc.getIndex()))
        {
            log.error("文档删除失败，删除的文档参数不能为空，请求参数数据：{}", Objects.isNull(deleteDoc) ?
                    "null" :
                    JSONObject.toJSONString(deleteDoc));
            return false;
        }
        String index = deleteDoc.getIndex().toLowerCase();
        String id = deleteDoc.getId();
        String routing = deleteDoc.getRouting();

        boolean deleteFlag = false;
        DeleteRequest request = new DeleteRequest(index, id);
        if (StringUtils.isNotEmpty(routing))
        {
            request.routing(routing);
        }

        if (async)
        {
            restHighLevelClient.deleteAsync(request, RequestOptions.DEFAULT, new ActionListener<DeleteResponse>()
            {
                @Override
                public void onResponse(DeleteResponse deleteResponse)
                {
                    if (deleteResponse.status() != RestStatus.OK)
                    {
                        log.error("删除文档出现错误，索引：{}，文档ID：{}，返回状态吗：{}", index, id, deleteResponse.status().getStatus());
                    }
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
                }
            });
            deleteFlag = true;
        }
        else
        {
            try
            {
                DeleteResponse deleteResponse = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
                deleteFlag = deleteResponse.status() == RestStatus.OK;
            }
            catch (Exception e)
            {
                log.error("删除文档出现错误，索引：{}，文档ID：{}", index, id);
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.DELETE_DOCUMENT);
            }
        }
        return deleteFlag;
    }

    @Override
    public boolean batchDeleteById(List<DocumentDTO> deleteDocs, boolean async) {
        if (CollectionUtils.isEmpty(deleteDocs))
        {
            log.error("批量删除文档失败，请求的文档列表为空!");
            return false;
        }

        BulkRequest bulkRequest = new BulkRequest();
        ///        List<ElasticsearchDocDo> errorDocs = new ArrayList<>();
        deleteDocs.forEach(elasticsearchDocDo ->
        {
            final String index = elasticsearchDocDo.getIndex();
            final String id = elasticsearchDocDo.getId();
            if (StringUtils.isBlank(index) || StringUtils.isBlank(id))
            {
                ///               errorDocs.add(elasticsearchDocDo);
                log.warn("删除当前文档出现错误，索引：{}和文档ID：{}不能为空！", index, id);
                return;
            }
            bulkRequest.add(new DeleteRequest(index, id));
        });

        if (async)
        {
            restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, new ActionListener<BulkResponse>()
            {
                @Override
                public void onResponse(BulkResponse bulkItemResponses)
                {
                    bulkItemResponses.forEach(bulkItemResponse ->
                    {
                        if (bulkItemResponse.status() != RestStatus.OK)
                        {
                            log.error("删除文档出现错误，索引：{}，文档ID：{}，错误消息：{}", bulkItemResponse.getIndex(), bulkItemResponse
                                    .getId(), bulkItemResponse.getFailureMessage());
                        }
                    });
                }

                @Override
                public void onFailure(Exception e)
                {
                    log.error("异步批量删除文档发生异常!");
                    SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_DELETE_DOCUMENT);
                }
            });
        }
        else
        {
            try
            {
                final BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                bulk.forEach(bulkItemResponse ->
                {
                    if (bulkItemResponse.isFailed())
                    {
                        log.error("删除文档失败，文档ID：{}，当前索引：{}，失败信息：{}", bulkItemResponse.getId(), bulkItemResponse
                                .getIndex(), bulkItemResponse.getFailureMessage());
                    }
                });
            }
            catch (Exception e)
            {
                SearchExceptionUtil.exceptionDetail(e, ElasticsearchOperationEnum.BATCH_DELETE_DOCUMENT);
            }

        }
        return true;
    }

    @Override
    public List<String> getIkList(String keyword, String index, String field) {
        List<String> list = new ArrayList<>();
        try
        {
            AnalyzeRequest request = AnalyzeRequest.withField(index, field, keyword);
            AnalyzeResponse response = restHighLevelClient.indices().analyze(request, RequestOptions.DEFAULT);
            List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
            for (AnalyzeResponse.AnalyzeToken analyzeToken : tokens)
            {
                list.add(analyzeToken.getTerm());
            }
        }
        catch (IOException e)
        {
            log.info("获取keywords失败！");
        }
        return list;
    }

}
