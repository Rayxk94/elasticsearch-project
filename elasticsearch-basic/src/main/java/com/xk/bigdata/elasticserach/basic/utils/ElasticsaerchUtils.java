package com.xk.bigdata.elasticserach.basic.utils;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

public class ElasticsaerchUtils {

    public static RestHighLevelClient client = null;

    /**
     * 初始化 客户端
     */
    public static void initClient(String hostName, int port) {
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(hostName, port, "http")));
    }

    /**
     * 关闭客户端
     */
    public static void closeClient() throws IOException {
        if (null != client) {
            client.close();
        }
    }

    /**
     * put 数据
     */
    public static IndexResponse put(IndexRequest request) throws IOException {
        return client.index(request, RequestOptions.DEFAULT);
    }

    /**
     * get 数据
     */
    public static GetResponse get(GetRequest getRequest) throws IOException {
        return client.get(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * 得到多条数据
     */
    public static MultiGetResponse mget(MultiGetRequest request) throws IOException {
        return client.mget(request, RequestOptions.DEFAULT);
    }

    /**
     * 数据是否存在
     */
    public static boolean isExists(GetRequest getRequest) throws IOException {
        return client.exists(getRequest, RequestOptions.DEFAULT);
    }

    /**
     * 更新数据：可以只跟新一个字段
     */
    public static UpdateResponse update(UpdateRequest request) throws IOException {
        return client.update(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除数据
     */
    public static DeleteResponse delete(DeleteRequest request) throws IOException {
        return client.delete(request, RequestOptions.DEFAULT);
    }

    public static SearchResponse search(SearchRequest searchRequest) throws IOException {
        return client.search(searchRequest, RequestOptions.DEFAULT);
    }

}
