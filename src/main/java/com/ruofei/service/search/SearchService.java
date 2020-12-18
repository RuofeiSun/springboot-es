package com.ruofei.service.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.List;
import java.util.Map;

/**
 * @Author: srf
 * @Date: 2020/12/17 18:29
 * @description:查询相关api
 */
public interface SearchService {

    /**
     * 根据索引和条件查询
     * @param index
     * @param conditions
     * @return
     */
    SearchResponse searchDoc(String index, Map<String, List<String>> conditions);

    /**
     * 根据索引和searchSourceBuilder查询
     * @param index
     * @param searchSourceBuilder
     * @return
     */
    SearchResponse searchDoc(String index, SearchSourceBuilder searchSourceBuilder);

    /**
     * 返回查询对象
     * @param index
     * @param conditions
     * @param clazz
     * @param <T>
     * @return
     */
    <T> List<T> searchDoc( String index,  Map<String, List<String>> conditions,  Class<T> clazz);

}
