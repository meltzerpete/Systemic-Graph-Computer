package graphEngine;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import java.util.concurrent.atomic.AtomicInteger;

import static graphEngine.Components.FITS1;

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
        Iterable<Relationship> fits1s = context.getRelationships(FITS1);
        return getRandomEndNode(fits1s);
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

    /**
     * Selects a random EndNode from an {@link ResourceIterator} of relationships.
     * @param relationships The relationships to iterate over
     * @return the randomly selected {@link Node}
     */
    private Node getRandomEndNode(Iterable<Relationship> relationships) {

        try {
            ResourceIterable<Relationship> rels = (ResourceIterable<Relationship>) relationships;

            AtomicInteger count = new AtomicInteger(2);
            return rels.stream().reduce((acc, next) ->
                    Math.random() > 1.0 / count.getAndIncrement() ? acc : next).get().getEndNode();
        } catch (Exception e){
            Computer.log.error(e.getMessage());
            throw new AssertionError("This should not happen")
        }

    }
}
