package com.ruofei.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Author: srf
 * @Date: 2020/12/9 20:27
 * @description:
 */
@Configuration
public class ESConfig implements DisposableBean {

    private RestHighLevelClient restHighLevelClient;

    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost("127.0.0.1", 9200, "http"));

        //配置超时时间（非必须）
        restClientBuilder.setRequestConfigCallback(requestConfigBuilder->{
            //连接超时时间
            requestConfigBuilder.setConnectTimeout(RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS);
            //socket超时时间
            requestConfigBuilder.setSocketTimeout(RestClientBuilder.DEFAULT_SOCKET_TIMEOUT_MILLIS);
            //请求超时时间
            requestConfigBuilder.setConnectionRequestTimeout(RestClientBuilder.DEFAULT_CONNECT_TIMEOUT_MILLIS);
            return requestConfigBuilder;
        });

        //异步 httpclient 连接参数配置
        restClientBuilder.setHttpClientConfigCallback(httpClientBuilder->{
            //最大连接数
            httpClientBuilder.setMaxConnTotal(RestClientBuilder.DEFAULT_MAX_CONN_TOTAL);
            //单主机并发最大数
            httpClientBuilder.setMaxConnPerRoute(RestClientBuilder.DEFAULT_MAX_CONN_PER_ROUTE);
            return httpClientBuilder;
        });

        // 鉴权设置，如果需要账号密码用下面逻辑
/*        if (StringUtils.isNotBlank(elasticsearchProperties.getUsername()) && StringUtils
                .isNotBlank(elasticsearchProperties.getPassword()))
        {
            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider
                    .setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(elasticsearchProperties
                            .getUsername(), elasticsearchProperties.getPassword()));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }*/

        restHighLevelClient = new RestHighLevelClient(restClientBuilder);

        return restHighLevelClient;
    }

    @Override
    public void destroy() throws Exception {
        restHighLevelClient.close();
    }
}
