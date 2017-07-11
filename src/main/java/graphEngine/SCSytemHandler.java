package graphEngine;

import org.neo4j.graphdb.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static graphEngine.Components.*;
import static graphEngine.Computer.db;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class SCSytemHandler {

    Stream<Node> getAllSystemsInScope(Node scope) {

        ResourceIterator<Relationship> relationships =
                (ResourceIterator<Relationship>)
                        scope.getRelationships(CONTAINS, Direction.OUTGOING).iterator();

        return relationships.stream().map(relationship -> relationship.getEndNode());
    }

    Stream<Node> getContextsInScope(Node scope) {

        return getAllSystemsInScope(scope).filter(node -> node.hasLabel(CONTEXT));
    }

    /**
     * Selects a random EndNode from an {@link ResourceIterable} of relationships.
     * @param relationships The relationships to iterate over
     * @return the randomly selected {@link Node}
     */
    private Node getRandomEndNode(Stream<Relationship> relationships) {

        AtomicInteger count = new AtomicInteger(2);
        return relationships.reduce((acc, next) ->
                Math.random() > 1.0 / count.getAndIncrement() ? acc : next).get().getEndNode();
    }

    private Node getRandomNode(Stream<Node> nodes) {

        AtomicInteger count = new AtomicInteger(2);
        return  nodes.reduce((acc, next) ->
                Math.random() > 1.0 / count.getAndIncrement() ? acc : next).get();
    }

    Node getRandomReady() throws NodeNotFoundException {

        ResourceIterator<Node> nodes = db.findNodes(READY);

        if (!nodes.hasNext())
            throw new NodeNotFoundException("No READY nodes found");

        return getRandomNode(db.findNodes(READY).stream());
    }

    Node getRandomS1(Node context) throws NodeNotFoundException {

        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) context.getRelationships(FITS1, Direction.OUTGOING);

        if (!relationships.iterator().hasNext())
            throw new NodeNotFoundException("No S1 matches found");

        return getRandomEndNode(relationships.stream());
    }

    Node getRandomS2(Node context) throws NodeNotFoundException {

        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) context.getRelationships(FITS2, Direction.OUTGOING);

        if (!relationships.iterator().hasNext())
            throw new NodeNotFoundException("No S2 matches found");

        return getRandomEndNode(relationships.stream());
    }

    Stream<Node> getParentScopes(Node node) {
        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) node.getRelationships(CONTAINS, Direction.INCOMING);

        return relationships.stream().map(relationship -> relationship.getEndNode());
    }
}
