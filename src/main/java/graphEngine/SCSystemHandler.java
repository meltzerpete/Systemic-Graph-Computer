package graphEngine;

import org.neo4j.graphdb.*;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static graphEngine.Components.*;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
abstract class SCSystemHandler {

    private Computer comp;
    private GraphDatabaseService db;

    SCSystemHandler(Computer comp, GraphDatabaseService db) {
        this.comp = comp;
        this.db = db;
    }

    /**
     * Finds all nodes contained in the given scope.
     * @param scope {@link Node} to look inside
     * @return {@link Stream} of contained {@link Node}s
     */
    Stream<Node> getAllSystemsInScope(Node scope) {

        ResourceIterator<Relationship> relationships =
                (ResourceIterator<Relationship>)
                        scope.getRelationships(CONTAINS, Direction.OUTGOING).iterator();

        return relationships.stream().map(Relationship::getEndNode);
    }

    /**
     * Finds all nodes contained in the given scope with CONTEXT {@link Label}.
     * @param scope {@link Node} to look inside
     * @return {@link Stream} of contained CONTEXT {@link Node}s
     */
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
        //noinspection ConstantConditions
        return relationships.reduce((acc, next) ->
                Math.random() > 1.0 / count.getAndIncrement() ? acc : next).get().getEndNode();
    }

    /**
     * Selects a random {@link Node} from a {@link Stream} of {@link Node}s.
     * @param nodes {@link Stream} of {@link Node}s
     * @return Randomly selected {@link Node}
     */
    private Node getRandomNode(Stream<Node> nodes) {

        AtomicInteger count = new AtomicInteger(2);
        //noinspection ConstantConditions
        return  nodes.reduce((acc, next) ->
                Math.random() > 1.0 / count.getAndIncrement() ? acc : next).get();
    }

    /**
     * Selects a random {@link Node} with the READY {@link Label}.
     * @return Randomly selected READY {@link Node}
     */
    Node getRandomReady() {

        // TODO perhaps change to indexed queue system
        return getRandomNode(db.findNodes(READY).stream());
    }

    /**
     * Selectes a random SCHEMA_1 matching {@link Node} for the given context.
     * @param context Context {@link Node} for which to find a match
     * @return Randomly selected SCHEMA_1 matching {@link Node}
     */
    Node getRandomS1(Node context) {

        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) context.getRelationships(FITS1, Direction.OUTGOING);

        return getRandomEndNode(relationships.stream());
    }

    /**
     * Selectes a random SCHEMA_1 matching {@link Node} for the given context.
     * @param context Context {@link Node} for which to find a match
     * @return Randomly selected SCHEMA_1 matching {@link Node}
     */
    Node getRandomS2(Node context) {

        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) context.getRelationships(FITS2, Direction.OUTGOING);

        return getRandomEndNode(relationships.stream());
    }

    /**
     * Get all parent scopes for the given {@link Node}.
     * @param node {@link Node} for which to find containing scopes
     * @return {@link Stream} of scope {@link Node}s
     */
    Stream<Node> getParentScopes(Node node) {
        ResourceIterator<Relationship> relationships =
                (ResourceIterator<Relationship>) node.getRelationships(CONTAINS, Direction.INCOMING).iterator();

        return relationships.stream().map(Relationship::getStartNode);
    }
}
