package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import io.searchbox.indices.aliases.GetAliases;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ElasticSearchEventHandlerIntegrationTest {

    public static final String LABEL = "MyLabel";
    public static final String INDEX = "mylabel";
    private static final  String serverUri = "http://10.201.50.36:9200";
    public static final String INDEX_SPEC = INDEX + ":" + LABEL + "(foo)";
    private GraphDatabaseService db;
    private JestClient client;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUri)
                .build());
        client = factory.getObject();
        db = new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(config())
                .newGraphDatabase();

        // create index
        client.execute(new CreateIndex.Builder(INDEX).build());
    }

    private Map<String, String> config() {
        return stringMap(
                "elasticsearch.host_name", serverUri,
                "elasticsearch.index_spec", INDEX_SPEC);
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.shutdownClient();
        db.shutdown();
    }

//    @Test
//    public void testAfterCommit() throws Exception {
//        Transaction tx = db.beginTx();
//        org.neo4j.graphdb.Node node = db.createNode(Label.label(LABEL));
//        node.setProperty("foo", "foobar");
//        node.setProperty("sketchID", 1000001);
//        String id = String.valueOf(node.getProperty("sketchID"));
//        tx.success();
//        tx.close();
//
//        Thread.sleep(1000); // wait for the async elasticsearch query to complete
//        JestResult response = client.execute(new GetAliases.Builder().build());
//        System.out.println(response.getJsonObject());
//        response = client.execute(new Get.Builder(INDEX, id).build());
//
//        assertEquals("request failed "+response.getErrorMessage(),true, response.isSucceeded());
//        assertEquals(INDEX, response.getValue("_index"));
//        assertEquals(id, response.getValue("_id"));
//        assertEquals(INDEX+"Sync", response.getValue("_type"));
//
//
//        Map source = response.getSourceAsObject(Map.class);
////        assertEquals(asList(LABEL), source.get("labels"));
////        assertEquals(id, source.get("id"));
//        assertEquals("foobar", source.get("foo"));
//    }
}
