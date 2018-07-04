package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Get;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class ElasticSearchEventHandlerTest {

    public static final String INDEX = "label";
    public static final String LABEL = "Label";
    private ElasticSearchEventHandler handler;
    private String serverUri = "http://10.201.50.36:9200";
    private ElasticSearchIndexSettings indexSettings;
    private GraphDatabaseService db;
    private JestClient client;
    private Node node;
    private String id;

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(serverUri)
                .multiThreaded(true)
                .build());
        client = factory.getObject();
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        Map<String, List<ElasticSearchIndexSpec>> indexSpec =
                ElasticSearchIndexSpecParser.parseIndexSpec(INDEX + ":" + LABEL + "(foo)");
        indexSettings = new ElasticSearchIndexSettings(indexSpec, true, true);
        
        handler = new ElasticSearchEventHandler(client, indexSettings);
        handler.setUseAsyncJest(false); // don't use async Jest for testing
        db.registerTransactionEventHandler(handler);
        
        client.execute(new CreateIndex.Builder(INDEX).build());
        node = createNode();
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.shutdownClient();
        db.unregisterTransactionEventHandler(handler);
        db.shutdown();
    }

    private Node createNode() {
        Transaction tx = db.beginTx();
        Node node = db.createNode(Label.label(LABEL));
        node.setProperty("foo", "bar");
        node.setProperty("sketchID", 1000001);
        tx.success();tx.close();
        id = "1000001";
        System.out.println("OOOOOOOO--------->" + id);
        return node;
    }
    
    private void assertIndexCreation(JestResult response) throws java.io.IOException {
        client.execute(new Get.Builder(INDEX, id).build());
        System.out.println("OOOOOOOO--------->" + response.getErrorMessage());
        System.out.println("OOOOOOOO--------->" + response.toString());
        assertEquals(true, response.isSucceeded());
        assertEquals(INDEX, response.getValue("_index"));
        assertEquals(id, response.getValue("_id"));
        assertEquals(INDEX+"Sync", response.getValue("_type"));
    }
    
//    @Test
//    public void testAfterCommit() throws Exception {
//        System.out.println("OOOOOOOO--------->" + INDEX);
//        System.out.println("OOOOOOOO--------->" + id);
//        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
//        assertIndexCreation(response);
//
//        Map source = response.getSourceAsObject(Map.class);
////        assertEquals(singletonList(LABEL), source.get("labels"));
////        assertEquals(id, source.get("id"));
//        System.out.println("OOOOOOOO--------->" + source.keySet());
//        assertEquals("bar", source.get("foo"));
//    }
//
//    @Test
//    public void testAfterCommitWithoutID() throws Exception {
//        client.execute(new DeleteIndex.Builder(INDEX).build());
//        indexSettings.setIncludeIDField(false);
//        client.execute(new CreateIndex.Builder(INDEX).build());
//        node = createNode();
//
//        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
//        assertIndexCreation(response);
//
//        Map source = response.getSourceAsObject(Map.class);
////        assertEquals(singletonList(LABEL), source.get("labels"));
////        assertEquals(null, source.get("id"));
//        assertEquals("bar", source.get("foo"));
//    }


    @Test
    public void testDelete() throws Exception {
        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
        assertIndexCreation(response);

        Transaction tx = db.beginTx();
        node = db.findNode(Label.label(LABEL), "sketchID", 1000001);
        System.out.println(node.getAllProperties());
        assertEquals("bar", node.getProperty("foo")); // check that we get the node that we just added
        node.delete();
        tx.success();
        tx.close();

        response = client.execute(new Get.Builder(INDEX, id).type(LABEL).build());
        assertEquals(false, response.getValue("found"));
    }

//    @Test
//    public void testUpdate() throws Exception {
//        JestResult response = client.execute(new Get.Builder(INDEX, id).build());
//        assertIndexCreation(response);
//
//        assertEquals("bar", response.getSourceAsObject(Map.class).get("foo"));
//
//        Transaction tx = db.beginTx();
//        node = db.findNode(Label.label(LABEL), "sketchID", 1000001);
//        node.setProperty("foo", "quux");
//        tx.success(); tx.close();
//
//        response = client.execute(new Get.Builder(INDEX, id).build());
//        System.out.println("22222222--------->" + response.getErrorMessage());
//        assertEquals(true,response.isSucceeded());
//        assertEquals(true, response.getValue("found"));
//        assertEquals("quux", response.getSourceAsObject(Map.class).get("foo"));
//    }
}
