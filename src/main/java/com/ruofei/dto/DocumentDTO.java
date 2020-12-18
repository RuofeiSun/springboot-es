package com.ruofei.dto;

import lombok.Data;

import java.util.Map;

/**
 * @Author: srf
 * @Date: 2020/12/17 10:58
 * @description:es文档模型
 */
@Data
public class DocumentDTO {

    /**
     * 索引
     */
    private String index;

    /**
     * 文档ID
     */
    private String id;

    /**
     * 路由参数
     */
    private String routing;

    /**
     * 文档内容
     */
    private Map<String, Object> jsonMap;

}
