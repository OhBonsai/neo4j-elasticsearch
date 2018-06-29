package org.neo4j.elasticsearch;

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
        Map<IndexId, BulkableAction> actions = new HashMap<>(1000);

        for (Node node : transactionData.createdNodes()) {
            System.out.println("OOOOOOOOOOOOO-----------> " + "Create node before commit");
            actions.putAll(indexRequests(node));
        }

        for (LabelEntry labelEntry : transactionData.removedLabels()) {
            actions.putAll(deleteRequests(labelEntry.node(), labelEntry.label()));
        }


        for (PropertyEntry<Node> propEntry : transactionData.assignedNodeProperties()) {
            actions.putAll(indexRequests(propEntry.entity()));
        }

        for (PropertyEntry<Node> propEntry : transactionData.removedNodeProperties()) {
            if (!transactionData.isDeleted(propEntry.entity()))
                actions.putAll(updateRequests(propEntry.entity()));
        }
        return actions.isEmpty() ? Collections.<BulkableAction>emptyList() : actions.values();
    }

    public void setUseAsyncJest(boolean useAsyncJest) {
        this.useAsyncJest = useAsyncJest;
    }

    @Override
    public void afterCommit(TransactionData transactionData, Collection<BulkableAction> actions) {
        if (actions.isEmpty()) {
            System.out.println("OOOOOOOOOOOOO-----------> " + "4");
            return;
        }
        try {
            Bulk bulk = new Bulk.Builder()
                    .addAction(actions).build();
            if (useAsyncJest) {
                client.executeAsync(bulk, this);
                System.out.println("OOOOOOOOOOOOO-----------> " + "5");
            }
            else {
                System.out.println("OOOOOOOOOOOOO-----------> " + "6");
                client.execute(bulk);
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, "Error updating ElasticSearch ", e);
        }
    }


    private Map<IndexId, Index> indexRequests(Node node) {
        HashMap<IndexId, Index> reqs = new HashMap<>();
        System.out.println("OOOOOOOOOOOOO-----------> " + "2");
        for (Label l: node.getLabels()) {
            String id = id(node), indexName = l.name().toLowerCase();
            reqs.put(new IndexId(indexName, id), new Index.Builder(nodeToJson(node))
                    .index(indexName)
                    .type(indexName+"Sync")
                    .id(id)
                    .build());
        }
        System.out.println("OOOOOOOOOOOOO-----------> " + "3");
        return reqs;
    }

    private Map<IndexId, Delete> deleteRequests(Node node, Label label) {
        HashMap<IndexId, Delete> reqs = new HashMap<>();

        String id = id(node), indexName = label.name().toLowerCase();
        reqs.put(new IndexId(label.name().toLowerCase(), id),
                 new Delete.Builder(id).index(indexName).build());
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
        try {
            System.out.println("OOOOOOOOOOOOO-----------> " + "2.1");
            String value = String.valueOf(node.getProperty("sketchID"));
            System.out.println(value);
            return value;
        }catch (Exception e){
            System.out.println("OOOOOOOOOOOOO-----------> " + "2.3");
            e.printStackTrace();
            return String.valueOf(node.getId());
        }
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
