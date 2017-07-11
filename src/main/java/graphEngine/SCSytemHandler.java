package graphEngine;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class SCSytemHandler {
    Node getRandomReady() {
        //TODO getRandomReady()
        return null;
    }

    Node getRandomS1(Node context) {
        //TODO getRandomS1()
        return null;
    }

    Node getRandomS2(Node context) {
        //TODO getRanodmS2()
        return null;
    }

    ResourceIterator<Node> getParentScopes(Node node) {
        //TODO getParentScope()
        return null;
    }

    ResourceIterator<Node> getContextsInScope(Node scope) {
        //TODO getContextsInScope()
        return null;
    }

    ResourceIterator<Node> getAllSystemsInScope(Node scope) {
        //TODO getAllSystemsInScope()
        return null;
    }

    private Node getRandomEndNode(ResourceIterator<Relationship> relationships) {
        AtomicInteger count = new AtomicInteger(2);
        relationships.stream().reduce((acc, next) -> {
            acc = Math.random() > 1.0 / count.getAndIncrement() ? acc : next;
        });
        return null;
    }
}
