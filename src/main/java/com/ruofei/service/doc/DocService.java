package com.ruofei.service.doc;

import com.ruofei.dto.DocumentDTO;

import java.util.List;

/**
 * @Author: srf
 * @Date: 2020/12/17 10:46
 * @description:Document相关API
 */
public interface DocService {

    /**
     * 添加文档
     * @param doc
     * @param async
     * @return
     */
    boolean addDoc(DocumentDTO doc, boolean async);

    /**
     * 批量添加文档
     * @param docDataList
     * @param async
     * @return
     */
    boolean addJsonDoc(List<DocumentDTO> docDataList, boolean async);

    /**
     * 文档ID单个更新
     * @param updateDoc
     * @param async
     * @return
     */
    boolean updateDoc(DocumentDTO updateDoc, boolean async);

    /**
     * 文档ID批量更新
     * @param updateDocList
     * @param async
     * @return
     */
    boolean batchUpdateDoc(List<DocumentDTO> updateDocList, boolean async);

    /**
     * 根据id删除文档
     * @param id
     * @param index
     * @param async
     * @return
     */
    boolean deleteById(String id, String index, boolean async);

    /**
     * 根据id删除文档
     * @param deleteDoc
     * @param async
     * @return
     */
    boolean deleteById(DocumentDTO deleteDoc, boolean async);

    /**
     * 批量删除
     * @param deleteDocs
     * @param async
     * @return
     */
    boolean batchDeleteById(List<DocumentDTO> deleteDocs, boolean async);

    /**
     * 获取keyword 分词结果
     *@param keyword 关键字
     *@param index 索引名
     *@param field 字段名
     *
     */
    List<String> getIkList(String keyword,String index,String field);

}
