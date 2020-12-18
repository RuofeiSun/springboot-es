package com.ruofei.service.search.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ruofei.service.search.SearchService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
 * @Date: 2020/12/17 18:29
 * @description:
 */
@Slf4j
@Service
public class SearchServiceImpl implements SearchService {

    @Autowired
    RestHighLevelClient restHighLevelClient;

    @Override
    public SearchResponse searchDoc(String index, Map<String, List<String>> conditions) {
        if (StringUtils.isEmpty(index) || CollectionUtils.isEmpty(conditions))
        {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数：【{}】不能为空!", index, JSONObject.toJSONString(conditions));
            return null;
        }

        index = index.toLowerCase();
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.from(0);
        searchSourceBuilder.size(1000);

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        conditions.forEach((key, value) -> queryBuilder.must(QueryBuilders.termsQuery(key, value)));
        searchSourceBuilder.query(queryBuilder);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = null;

        try
        {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        }
        catch (IOException e)
        {
            log.error("根据查询条件【{}】搜索商品出现异常", JSON.toJSON(conditions), e);
        }
        return search;
    }

    @Override
    public SearchResponse searchDoc(String index, SearchSourceBuilder searchSourceBuilder) {
        if (StringUtils.isEmpty(index) || null == searchSourceBuilder)
        {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数不能为空!", index);
            return null;
        }

        index = index.toLowerCase();
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse search = null;

        try
        {
            search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        }
        catch (IOException e)
        {
            log.error("根据查询条件搜索商品出现异常", e);
        }
        return search;
    }

    @Override
    public <T> List<T> searchDoc(String index, Map<String, List<String>> conditions, Class<T> clazz) {
        if (StringUtils.isBlank(index) || CollectionUtils.isEmpty(conditions) || Objects.isNull(clazz))
        {
            log.error("根据条件获取文档失败，请求参数索引：【{}】，参数：【{}}，返回类型：【{}】不能为空!", index, JSONObject
                    .toJSONString(conditions), clazz);
            return null;
        }

        SearchResponse search = this.searchDoc(index, conditions);

        List<T> docList = new ArrayList<>();
        if (null != search)
        {
            SearchHits hits = search.getHits();
            if (search.status() == RestStatus.OK)
            {
                if (hits.getTotalHits().value > 0)
                {
                    hits.forEach(hit -> docList.add(JSON.parseObject(hit.getSourceAsString(), clazz)));
                }
            }
            else
            {
                log.error("搜索返回状态码:{}", search.status().getStatus());
            }
        }
        return docList;
    }

}
