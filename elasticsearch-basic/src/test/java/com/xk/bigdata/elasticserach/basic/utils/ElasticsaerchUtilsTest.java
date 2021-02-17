package com.xk.bigdata.elasticserach.basic.utils;

import com.alibaba.fastjson.JSON;
import com.xk.bigdata.elasticserach.basic.domain.Person;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.IntStream;

public class ElasticsaerchUtilsTest {

    public static final String HOSTNAME = "bigdata";

    public static final int PORT = 9200;

    private static final String INDEX = "bigdata";

    @Before
    public void stepUp() {
        ElasticsaerchUtils.initClient(HOSTNAME, PORT);
    }

    @After
    public void cleanUp() throws IOException {
        ElasticsaerchUtils.closeClient();
    }

    /**
     * 插入一条 json 的 string
     */
    @Test
    public void testPut01() throws IOException {
        IndexRequest request = new IndexRequest(INDEX);
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"bigdata\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);
        IndexResponse response = ElasticsaerchUtils.put(request);
        System.out.println(response);
    }

    /**
     * 插入一个  map 结构数据
     */
    @Test
    public void testPut02() throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("name", "spark");
        jsonMap.put("age", 18);
        jsonMap.put("tags", new String[]{"爱学习", "看电影"});

        IndexRequest request = new IndexRequest(INDEX)
                .id("2").source(jsonMap);
        IndexResponse response = ElasticsaerchUtils.put(request);
        System.out.println(response);
    }

    /**
     * 把 class 里面的数据插入 es
     *
     * @throws IOException
     */
    @Test
    public void testPut03() throws IOException {
        Person person = new Person("flink", 31);

        IndexRequest request = new IndexRequest(INDEX)
                .id("3").source(JSON.toJSONString(person), Requests.INDEX_CONTENT_TYPE);
        IndexResponse response = ElasticsaerchUtils.put(request);
        System.out.println(response);
    }

    @Test
    public void testPut04() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("name", "hadoop");
            builder.field("age", 100);
        }
        builder.endObject();

        IndexRequest request = new IndexRequest(INDEX)
                .id("4").source(builder);
        IndexResponse response = ElasticsaerchUtils.put(request);
        System.out.println(response);
    }

    @Test
    public void testGetById01() throws IOException {
        GetRequest getRequest = new GetRequest(INDEX, "1");
        GetResponse getResponse = ElasticsaerchUtils.get(getRequest);
        System.out.println(getResponse.getSourceAsString());
    }

    @Test
    public void testGetById02() throws IOException {
        GetRequest getRequest = new GetRequest(INDEX, "2");
        // 选择只看哪些字段
        String[] includes = Strings.EMPTY_ARRAY;
        // 选择排除哪些字段
        String[] excludes = new String[]{"age", "tags"};
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        getRequest.fetchSourceContext(fetchSourceContext);
        GetResponse getResponse = ElasticsaerchUtils.get(getRequest);
        System.out.println(getResponse.getSourceAsString());
    }

    @Test
    public void testGetByIds() throws IOException {
        MultiGetRequest request = new MultiGetRequest();
        request.add(new MultiGetRequest.Item(INDEX, "2"));
        request.add(new MultiGetRequest.Item(INDEX, "3"));
        request.add(new MultiGetRequest.Item(INDEX, "4"));
        MultiGetResponse response = ElasticsaerchUtils.mget(request);
        MultiGetItemResponse[] responses = response.getResponses();
        for (MultiGetItemResponse item : responses) {
            System.out.println(item.getResponse().getSourceAsString());
        }
    }

    @Test
    public void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest(INDEX, "1");
        boolean result = ElasticsaerchUtils.isExists(getRequest);
        System.out.println(result);
    }

    @Test
    public void testUpdate01() throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("sex", "男");
        UpdateRequest request = new UpdateRequest(INDEX, "4").doc(jsonMap);
        UpdateResponse updateResponse = ElasticsaerchUtils.update(request);
        System.out.println(updateResponse);
    }

    @Test
    public void testUpdate02() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("score", 99);
        }
        builder.endObject();
        UpdateRequest request = new UpdateRequest(INDEX, "4").doc(builder);
        UpdateResponse updateResponse = ElasticsaerchUtils.update(request);
        System.out.println(updateResponse);
    }

    /**
     * 删除一条数据
     */
    @Test
    public void testDelete() throws IOException {
        DeleteRequest request = new DeleteRequest(INDEX, "1");
        DeleteResponse deleteResponse = ElasticsaerchUtils.delete(request);
        System.out.println(deleteResponse);
    }

    @Test
    public void testMakeData() throws IOException {
        Random random = new Random();
        String[] strings = {"语文", "数学", "英语"};
        for (int i = 0; i <= 100; i++) {
            Map<String, Object> jsonMap = new HashMap<>();
            jsonMap.put("name", "spark" + i);
            jsonMap.put("age", random.nextInt(5) + 10);
            jsonMap.put("subject", strings[random.nextInt(3)]);
            jsonMap.put("score", random.nextInt(40) + 60);
            IndexRequest request = new IndexRequest(INDEX)
                    .id(i + "").source(jsonMap);
            System.out.println(jsonMap);
            IndexResponse response = ElasticsaerchUtils.put(request);
        }
    }


}
