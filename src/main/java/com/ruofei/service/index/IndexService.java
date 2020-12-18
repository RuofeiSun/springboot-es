package com.ruofei.service.index;

import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Set;

/**
 * @Author: srf
 * @Date: 2020/12/16 11:31
 * @description:索引相关操作
 */
public interface IndexService {

    /**
     * 创建索引
     * @param index: 索引名称
     * @param aliasIndex: 索引别名
     * @Param settings:索引配置
     * @param mappings: 索引配置
     * @return
     */
    boolean createIndex(String index,String aliasIndex, Settings.Builder settings, XContentBuilder mappings) throws IOException;

    /**
     *删除索引
     * @param index
     * @param async
     * @return
     */
    boolean deleteIndex(String index, boolean async);

    /**
     * 重建索引
     * @param targetIndex
     * @param destIndex
     */
    void asyncReindex(String[] targetIndex, String destIndex) throws IOException;

    /**
     *重新索引任务提交
     *
     *   也可以使用Task API提交一个eindexRequest，而不是等待它完成。这相当于将等待完成标志设置为false的REST请求
     * @param targetIndex
     * @param destIndex
     * @return
     */
    String createTaskReindex(String[] targetIndex, String destIndex) throws IOException;

    /**
     * 设置索引别名
     * @param index
     * @param alias
     * @return
     */
    boolean setIndexAliases(String index, String alias) throws IOException;

    /**
     * 获取索引别名
     * @param index
     * @return
     */
    Set<AliasMetaData> getIndexAliases(String index) throws IOException;

    /**
     * 删除索引别名
     * @param index
     * @param alias
     * @return
     */
    boolean removeIndexAliases(String index, String alias) throws IOException;

    /**
     * 关闭索引
     * @param index
     * @return
     */
    boolean closeIndex(String index);

    /**
     * 开启索引
     * @param index
     * @return
     */
    boolean openIndex(String index);

    /**
     * [简要描述]:索引是否存在
     * [详细描述]:
     *
     * @param index :
     * @return boolean
     **/
    boolean existIndex(String index) throws IOException;

}
