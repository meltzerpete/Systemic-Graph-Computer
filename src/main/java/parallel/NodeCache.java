package parallel;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.Iterators;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by Pete Meltzer on 09/08/17.
 */
class NodeCache {

    NodeDetails[] cache;
    ReentrantLock[] nodeLocks;

    NodeCache(long totalNoOfNodes) {
        cache = new NodeDetails[(int) totalNoOfNodes];
    }

    boolean upToDate(Node node, long nodeID) {
//        nodeLocks[(int) nodeID].lock();
//        try {

            NodeDetails target = cache[(int) nodeID];
            if (target == null)
                return true;

            if (!Iterators.stream(node.getLabels().iterator())
                    .allMatch(label -> target.labels.contains(label)))
                return false;

            if (!node.getAllProperties().entrySet().stream()
                    .allMatch(stringObjectEntry -> target.properties.contains(stringObjectEntry)))
                return false;

            if (!Iterators.stream((node.getRelationships().iterator()))
                    .allMatch(relationship -> target.relationships.contains(relationship)))
                return false;

            // node is up to date
            return true;
//
//        } finally {
//            nodeLocks[(int) nodeID].unlock();
//        }
    }

    void update(Node node, long nodeID) {
        cache[(int) nodeID] = new NodeDetails(
                Iterators.asSet(node.getLabels().iterator()),
                node.getAllProperties().entrySet(),
                Iterators.asSet(node.getRelationships().iterator()));
    }
}

class NodeDetails {
    NodeDetails(Set<Label> labels, Set<Map.Entry<String, Object>> properties, Set<Relationship> relationships) {
        this.labels = labels;
        this.properties = properties;
        this.relationships = relationships;
    }

    Set<Label> labels;
    Set<Map.Entry<String, Object>> properties;
    Set<Relationship> relationships;
}

