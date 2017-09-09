package graphEngine;

import common.Components;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static common.Components.*;

/**
 * Created by Pete Meltzer on 11/07/17.
 * <p>All auxiliary functions for getting Nodes from the DB.</p>
 *
 * <p>This class may not be instantiated directly, but should
 * be requested from the Computer class that initializes it
 * correctly.</p>
 */
abstract class SCSystemHandler {

    private GraphDatabaseService db;

    /**
     * Used only by extending classes to initialize the class correctly.
     * @param db GraphDatabaseService
     */
    SCSystemHandler(GraphDatabaseService db) {
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
     * Selects a random context {@link Node} with the READY {@link Label}.
     * @return Randomly selected READY {@link Node}
     */
    Node getRandomReady() throws NoMoreReadysException {

        // TODO perhaps change to indexed queue system

        AtomicInteger count = new AtomicInteger(2);
        //noinspection ConstantConditions
        return  db.findNodes(READY).stream()
                .reduce((acc, next)
                        -> Math.random() > 1.0 / count.getAndIncrement() ? acc : next)
                .orElseThrow(new NoMoreReadysException("No more READY nodes found."));
    }

    /**
     * Selectes a random SCHEMA_1 matching {@link Node} for the given context and within the given scope.
     * @param context Context {@link Node} for which to find a match
     * @return Randomly selected SCHEMA_1 matching {@link Node}
     */
    private Node getRandomS1(Node context, Node scope) {

        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) context.getRelationships(FITS1, Direction.OUTGOING);

        Stream<Relationship> fits = relationships.stream().filter(rel -> rel.getProperty("scope").equals(scope.getId()));

        return getRandomEndNode(fits);
    }

    /**
     * Selectes a random SCHEMA_2 matching {@link Node} for the given context.
     * @param context Context {@link Node} for which to find a match
     * @param s1
     * @return Randomly selected SCHEMA_1 matching {@link Node}
     */
    private Node getRandomS2(Node context, Node scope, Node s1) {

        ResourceIterable<Relationship> relationships =
                (ResourceIterable<Relationship>) context.getRelationships(FITS2, Direction.OUTGOING);

        Stream<Relationship> fits = relationships.stream()
                .filter(rel -> !rel.getEndNode().equals(s1))
                .filter(rel -> rel.getProperty("scope").equals(scope.getId()));

        return getRandomEndNode(fits);
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

    /**
     * Get a random pair of nodes that are ready for interaction in this context.
     * @param readyContext context Node
     * @return Pair of Nodes
     */
    Pair getRandomPair(Node readyContext) {

        Long[] idArray = ArrayUtils.toObject((long[]) readyContext.getProperty(Components.readyContextScopeID));
        HashSet<Long> idSet = new HashSet<>(Arrays.asList(idArray));

        List<Pair> pairs = new LinkedList<>();
        getParentScopes(readyContext)
                .filter(scope -> idSet.contains(scope.getId()))
                .forEach(scope -> {
            Pair readyPair = new Pair();
            try {
                readyPair.s1 = getRandomS1(readyContext, scope);
                readyPair.s2 = getRandomS2(readyContext, scope, readyPair.s1);
                pairs.add(readyPair);
            } catch (NoSuchElementException e) {
                e.printStackTrace();
                System.out.println(e.getClass());
                System.out.println(e.getMessage());
            }
        });

        AtomicInteger count = new AtomicInteger(2);
        //noinspection ConstantConditions
        return pairs.stream()
                .filter(Objects::nonNull)
                .reduce((acc, next) -> Math.random() > 1.0 / count.getAndIncrement() ? acc : next).get();
    }
}
