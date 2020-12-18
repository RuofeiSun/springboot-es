package com.ruofei.constants;

/**
 * 操作类型
 */
public enum ElasticsearchOperationEnum
{
    /**
     * 创建索引
     */
    CREATE_INDEX("创建索引"),
    /**
     * 删除索引
     */
    DELETE_INDEX("删除索引"),
    /**
     * 索引是否存在
     */
    EXIST_INDEX("索引是否存在"),

    /**
     * 添加文档
     */
    ADD_DOCUMENT("添加文档"),
    /**
     * 批量添加文档
     */
    BATCH_ADD_DOCUMENT("批量添加文档"),

    /**
     * 更新文档
     */
    UPDATE_DOCUMENT("更新文档"),
    /**
     * 批量更新文档
     */
    BATCH_UPDATE_DOCUMENT("批量更新文档"),

    /**
     * 搜索
     */
    SEARCH("搜索"),
    /**
     * 文档ID搜索
     */
    SEARCH_BY_ID("文档ID搜索"),

    /**
     * 删除文档
     */
    DELETE_DOCUMENT("删除文档"),
    /**
     * 批量删除文档
     */
    BATCH_DELETE_DOCUMENT("批量删除文档"),

    /**
     * 聚合
     */
    AGGREGATION("聚合"),

    /**
     * 条件统计
     */
    COUNT("条件统计"),

    /**
     * 重新索引
     */
    REINDEX("重新索引"),

    /**
     * 修改索引-添加自定义分词
     */
    UPDATE_INDEX_ANALYSIS("重新索引"),

    /**
     * 打开索引
     */
    OPEN_INDEX("打开索引"),

    /**
     * 关闭索引
     */
    CLOSE_INDEX("关闭索引"),

    PUT_MAPPING("设置索引mapping信息"),
    ;
    private String operationName;

    ElasticsearchOperationEnum(String operationName)
    {
        this.operationName = operationName;
    }

    public String getOperationName()
    {
        return operationName;
    }
}
