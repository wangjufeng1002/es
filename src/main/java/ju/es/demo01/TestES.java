package ju.es.demo01;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;

/**
 * @Author:ju
 * @Description:
 * @Date:Create in 19:56 2017-11-14
 */
public class TestES {

    private static TransportClient client = null;

    static {
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch")
                .put("client.transport.sniff", true)
                .build();
        try {
            client = new PreBuiltTransportClient(settings).addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.92.115"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建索引
     */
    /**
     *  {"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"jufeng":{"properties":{"type":{"type":"string","store":"yes"},"eventCount":{"type":"long","store":"yes"},"eventDate":{"type":"date","format":"dateOptionalTime","store":"yes"},"message":{"type":"string","index":"not_analyzed","store":"yes"}}}}}
     * @throws IOException
     */
    @Test
    public void createIndex() throws IOException {
        XContentBuilder mapping =  XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("settings")
                        .field("number_of_shards", 1) //设置分片数量
                        .field("number_of_replicas", 0) //设置副本数量
                    .endObject()


                    .startObject("mappings")
                        .startObject("jufeng")         //type 名称
                            .startObject("properties") //下面是设置文档列属性
                                .startObject("type")
                                    .field("type","string")
                                    .field("store","yes")
                                .endObject()
                                .startObject("eventCount")
                                    .field("type","long")
                                    .field("store","yes")
                                .endObject()
                                .startObject("eventDate")
                                     .field("type","date")
                                     .field("format","dateOptionalTime")
                                     .field("store","yes")
                                .endObject()
                                .startObject("message")
                                     .field("type","string")
                                     .field("index","not_analyzed")
                                    .field("store","yes")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject();

        System.out.println( mapping.string());

       /* CreateIndexRequestBuilder wang = client.admin().indices().prepareCreate("wang").setSource(mapping);
        CreateIndexResponse response = wang.execute().actionGet();

        if(response.isAcknowledged()){
            System.out.println(response.toString());
            System.out.println("Index created");
        }else{
            System.out.println("Index creation failed.");
        }*/

    }

    /**
     * {
        "type": "syslog",
        "eventCount": 1,
        "eventDate": "2017-11-14T15:24:59.819Z",
        "message": "secilog insert doc test"
        }
     * @throws IOException
     */
    /**
     * 增添文档
     * @throws IOException
     */
    @Test
    public void  addDoc() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                    .field("type", "syslog")
                    .field("eventCount",1)
                    .field("eventDate", new Date())
                    .field("message","secilog insert doc test")
                .endObject();
        IndexResponse response = client.prepareIndex("wang", "jufeng", "1")
                .setSource(source).get();

        System.out.println(source.string());
        System.out.println("==================================================================================");
        System.out.println("idnex: "+response.getIndex()+" id :" +response.getId());
    }

    /**
     * 更新文档
     * @throws IOException
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void updateDoc() throws IOException, ExecutionException, InterruptedException {
        UpdateRequest updateRequest = new UpdateRequest();
        updateRequest.index("wang");
        updateRequest.type("jufeng");
        updateRequest.id("1");
        updateRequest.doc(XContentFactory.jsonBuilder().startObject().field("type","title").endObject());
        client.update(updateRequest).get();
    }
}
