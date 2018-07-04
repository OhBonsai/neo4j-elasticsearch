package org.neo4j.elasticsearch;

import com.graphaware.tx.event.improved.api.ImprovedTransactionData;
import com.graphaware.tx.event.improved.api.LazyTransactionData;
import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.register.Register;


import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
* @author mh
* @since 25.04.15
*/
class ElasticSearchEventHandler implements TransactionEventHandler<Collection<BulkableAction>>, JestResultHandler<JestResult> {
    private final JestClient client;
    private final static Logger logger = Logger.getLogger(ElasticSearchEventHandler.class.getName());
    private final ElasticSearchIndexSettings indexSettings;
    private final Set<String> indexLabels;
    private boolean useAsyncJest = true;

    public ElasticSearchEventHandler(JestClient client, ElasticSearchIndexSettings indexSettings) {
        this.client = client;
        this.indexSettings = indexSettings;
        this.indexLabels = indexSettings.getIndexSpec().keySet();
    }

    @Override
    public Collection<BulkableAction> beforeCommit(TransactionData transactionData) throws Exception {
        ImprovedTransactionData improvedTransactionData = new LazyTransactionData(transactionData);

        Map<IndexId, BulkableAction> actions = new HashMap<>(1000);

        for (Node createNode : transactionData.createdNodes()) {
            actions.putAll(indexRequests(createNode));
        }


        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (!transactionData.isDeleted(propEntry.entity())){
                actions.putAll(updateRequests(propEntry.entity()));
            }
        }

        for (Node deleteNode : improvedTransactionData.getAllDeletedNodes()) {
            actions.putAll(deleteRequests(deleteNode));
        }

        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            actions.putAll(indexRequests(propEntry.entity()));
        }

        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions.values();
    }

    public void setUseAsyncJest(boolean useAsyncJest) {
        this.useAsyncJest = useAsyncJest;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Collection<BulkableAction> actions) {
        if (actions.isEmpty()) {
            
            return;
        }
        try {
            Bulk bulk = new Bulk.Builder()
                    .addAction(actions).build();
            if (useAsyncJest) {
                client.executeAsync(bulk, this);
            }
            else {
                JestResult reponse = client.execute(bulk);
                System.out.println(reponse.getErrorMessage());
                System.out.println(reponse.getJsonObject());
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error updating ElasticSearch ", e);
        }
    }


    private Map<IndexId, Index> indexRequests(Node node) {
        HashMap<IndexId, Index> reqs = new HashMap<>();
        
        for (Label l: node.getLabels()) {
            String id = id(node);
            String indexName = l.name().toLowerCase();
            reqs.put(new IndexId(indexName, id), new Index.Builder(nodeToJson(node))
                    .index(indexName)
                    .type(indexName+"Sync")
                    .id(id)
                    .build());
        }
        
        return reqs;
    }

    private Map<IndexId, Delete> deleteRequests(Node node, Label label) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();
        
        String id = id(node), indexName = label.name().toLowerCase();
        reqs.put(new IndexId(indexName, id),
                 new Delete.Builder(id).index(indexName).type(label.name()+"Sync").build());
    	return reqs;
    }

    private Map<IndexId, Delete> deleteRequests(String sketchID) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();
        reqs.put(new IndexId("testIndex", sketchID),
                new Delete.Builder(sketchID).build());
        return reqs;
    }

    private Map<IndexId, Delete> deleteRequests(Node node) {
        System.out.println("Delete nodes");
        System.out.println("--------------->" + node.toString());
        System.out.println("--------------->" + node.getAllProperties());

        HashMap<IndexId, Delete> reqs = new HashMap<>();
        String id = id(node);
        for (Label l: node.getLabels()) {
            String indexName = l.name().toLowerCase();
            reqs.put(new IndexId(indexName, id),
                    new Delete.Builder(id).
                            index(indexName)
                            .type(indexName+"Sync")
                            .build());
        }
        return reqs;
    }
    
    private Map<IndexId, Update> updateRequests(Node node) {
    	HashMap<IndexId, Update> reqs = new HashMap<>();
    	for (Label l: node.getLabels()) {
            String id = id(node), indexName = l.name().toLowerCase();
            reqs.put(new IndexId(indexName, id),
                    new Update.Builder(nodeToJson(node))
                              .type(l.name()+"Sync")
                              .index(indexName)
                              .id(id)
                              .build());

    	}
    	return reqs;
    }

    private String id(Node node) {
        System.out.println(node.getAllProperties());
        Map json = nodeToJson(node);
        System.out.println(json);
        return String.valueOf(json.getOrDefault("sketchID", node.getId()));
    }

    private Map nodeToJson(Node node) {
        Map<String, Object> json = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : node.getAllProperties().entrySet()) {
            json.put(entry.getKey(), entry.getValue());
        }
        return json;
    }

    @Override
    public void afterRollback(TransactionData transactionData, Collection<BulkableAction> actions) {
    }

    @Override
    public void completed(JestResult jestResult) {
        if (jestResult.isSucceeded() && jestResult.getErrorMessage() == null) {
            logger.fine("ElasticSearch Update Success");
        } else {
            logger.severe("ElasticSearch Update Failed: " + jestResult.getErrorMessage());
        }
    }

    @Override
    public void failed(Exception e) {
        logger.log(Level.WARNING,"Problem Updating ElasticSearch ",e);
    }
    
    private class IndexId {
        final String indexName, id;
        public IndexId(String indexName, String id) {
            this.indexName = indexName;
            this.id = id;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((id == null) ? 0 : id.hashCode());
            result = prime * result
                    + ((indexName == null) ? 0 : indexName.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (!(obj instanceof IndexId))
                return false;
            IndexId other = (IndexId) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (id == null) {
                if (other.id != null)
                    return false;
            } else if (!id.equals(other.id))
                return false;
            if (indexName == null) {
                if (other.indexName != null)
                    return false;
            } else if (!indexName.equals(other.indexName))
                return false;
            return true;
        }
        
        private ElasticSearchEventHandler getOuterType() {
            return ElasticSearchEventHandler.this;
        }

        @Override
        public String toString() {
            return "IndexId [indexName=" + indexName + ", id=" + id + "]";
        }
    }
}
