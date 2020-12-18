package com.ruofei.exception;

import com.ruofei.constants.ElasticsearchConstants;
import com.ruofei.constants.ElasticsearchOperationEnum;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.StatusLine;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.ResponseException;

/**
 * 异常类
 */
@Slf4j
public class SearchExceptionUtil
{
    /**
     * 异常处理
     * @param e
     * @param operationEnum
     */
    public static void exceptionDetail(Exception e, ElasticsearchOperationEnum operationEnum)
    {
        if (e instanceof ResponseException)
        {
            ResponseException re = (ResponseException) e;
            final StatusLine statusLine = re.getResponse().getStatusLine();
            int status = statusLine.getStatusCode();
            String reasonPhrase = statusLine.getReasonPhrase();
            statusInfo(status, reasonPhrase);
        }

        if (e instanceof ElasticsearchException)
        {
            ElasticsearchException elasticsearchException = (ElasticsearchException) e;
            int status = elasticsearchException.status().getStatus();
            String detailedMessage = elasticsearchException.getDetailedMessage();
            statusInfo(status, detailedMessage);
        }
        log.error("请求ES:{}操作发生异常，错误详情：", operationEnum.getOperationName(), e);
    }

    private static void statusInfo(int status, String detailedMessage)
    {
        if (status > ElasticsearchConstants.HTTP_500)
        {
            log.error("请求ES操作发生服务端5XX错误，错误状态码：{},错误消息：{}", status, detailedMessage);
        }
        else if (status > ElasticsearchConstants.HTTP_400)
        {
            log.error("请求ES操作发生服务端4XX错误，错误状态码：{},错误消息：{}", status, detailedMessage);
        }
        else
        {
            log.error("请求ES操作发生未知异常，错误状态码：{},错误消息：{}", status, detailedMessage);
        }
    }
}
