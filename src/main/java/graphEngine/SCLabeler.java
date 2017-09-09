package graphEngine;

import common.Components;
import nodeParser.NodeMatch;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 11/07/17.
 * <p>Handles all automatic creation of FITS Relationships
 * and Labeling of READY Contexts.</p>
 *
 * <p>This class may not be instantiated directly, but should
 * be requested from the Computer class that initializes it
 * correctly.</p>
 */
abstract class SCLabeler {

    private Computer comp;
    private GraphDatabaseService db;
    boolean debug = false;

    /**
     * Used only by extending classes to initialize the class correctly.
     * @param comp Instance of the Computer object
     * @param db GraphDatabaseService
     */
    SCLabeler(Computer comp, GraphDatabaseService db) {
        this.comp = comp;
        this.db = db;
    }

    /**
     * Checks if a Relationship of a given type already exists between two Nodes.
     * @param from StartNode
     * @param to EndNode
     * @param relationshipType the type of the relationship
     * @return {@code true} if relationship exists
     */
    private boolean relationshipExists(Node from, Node to, RelationshipType relationshipType) {
        ResourceIterator<Relationship> rels =
                (ResourceIterator<Relationship>) from.getRelationships(relationshipType, Direction.OUTGOING).iterator();
        return rels.stream()
                .anyMatch(rel -> rel.getEndNode().equals(to));
    }

    /**
     * Creates {@code FITS1} and {@code FITS2} {@link Relationship} between all matching
     * context Nodes and systems that fit according to the {@code s1Query} and
     * {@code s2Query} properties in the context within a given scope {@link Node}.
     * Also adds a property to the FITS Relationship to indicate which scope it
     * is contained in.
     * @param scope the scope {@code Node} in which to add FITS relationships
     */
    void createFitsInScope(Node scope) {
        //TODO can add extra matching conditions here - OR/AND?

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> containedContexts = scHandler.getContextsInScope(scope);
        containedContexts.forEach(context -> {

            if (debug) print("Context: %s", context.getProperty("name"));

            // S1
            if (debug) System.out.println("\nS1\n");

            if (context.hasProperty(Components.s1Query)) {
                String queryString = (String) context.getProperty(Components.s1Query);

                if (queryString.startsWith("(:")) {
                    scHandler.getAllSystemsInScope(scope)
                            .filter(target -> !target.equals(context))
                            .filter(target -> !relationshipExists(context, target, Components.FITS1))
                            .filter(target -> matchNode(comp.getNodeMatch(queryString), target))
                            .forEach(target -> {
                                Relationship rel = context.createRelationshipTo(target, Components.FITS1);
                                rel.setProperty("scope", scope.getId());
                            });
                } else {
                    // cypher query
                    ResourceIterator<Node> targetNodes = db.execute("match" + queryString + "return distinct n;").columnAs("n");
                    targetNodes.stream()
                            .filter(target -> relationshipExists(scope, target, Components.CONTAINS))
                            .filter(target -> !target.equals(context))
                            .filter(target -> !relationshipExists(context, target, Components.FITS1))
                            .forEach(target -> {
                                Relationship rel = context.createRelationshipTo(target, Components.FITS1);
                                rel.setProperty("scope", scope.getId());
                            });
                }
            }

            // S2
            if (debug) System.out.println("\nS2\n");

            if (context.hasProperty(Components.s2Query)) {
                String queryString = (String) context.getProperty(Components.s2Query);

                if (queryString.startsWith("(:")) {
                    scHandler.getAllSystemsInScope(scope)
                            .filter(target -> !target.equals(context))
                            .filter(target -> !relationshipExists(context, target, Components.FITS2))
                            .filter(target -> matchNode(comp.getNodeMatch(queryString), target))
                            .forEach(target -> {
                                Relationship rel = context.createRelationshipTo(target, Components.FITS2);
                                rel.setProperty("scope", scope.getId());
                            });
                } else {
                    // cypher query
                    ResourceIterator<Node> targetNodes = db.execute("match" + queryString + "return distinct n;").columnAs("n");
                    targetNodes.stream()
                            .filter(target -> relationshipExists(scope, target, Components.CONTAINS))
                            .filter(target -> !target.equals(context))
                            .filter(target -> !relationshipExists(context, target, Components.FITS2))
                            .forEach(target -> {
                                Relationship rel = context.createRelationshipTo(target, Components.FITS2);
                                rel.setProperty("scope", scope.getId());
                            });
                }
            }
        });
    }

    /**
     * Creates all FITS relationships for a single given Node by
     * matchign against all context systems contained in the same
     * scope(s).
     * @param target Node
     */
    void createFitsForTarget(Node target) {
        SCSystemHandler handler = comp.getHandler();
        Set<Node> targetScopes = Iterators.asSet(handler.getParentScopes(target).iterator());

        db.findNodes(Components.CONTEXT).stream()
                .filter(context -> !context.equals(target))
                .forEach(context -> {
                    Set<Node> contextScopes = Iterators.asSet(handler.getParentScopes(context).iterator());
                    contextScopes.retainAll(targetScopes);
                    if (contextScopes.isEmpty()) return;

                    // S1
                    if (context.hasProperty(Components.s1Query)) {
                        String queryString = (String) context.getProperty(Components.s1Query);

                        if (queryString.startsWith("(:")) {
                            // parsed node
                            contextScopes.stream()
                                    .filter(scope -> matchNode(comp.getNodeMatch(queryString), target))
                                    .forEach(scope -> {
                                        Relationship rel = context.createRelationshipTo(target, Components.FITS1);
                                        rel.setProperty("scope", scope.getId());
                            });

                        }
                    }

                    // S2
                    if (context.hasProperty(Components.s2Query)) {
                        String queryString = (String) context.getProperty(Components.s2Query);

                        if (queryString.startsWith("(:")) {
                            // parsed node
                            contextScopes.stream()
                                    .filter(scope -> matchNode(comp.getNodeMatch(queryString), target))
                                    .forEach(scope -> {
                                        Relationship rel = context.createRelationshipTo(target, Components.FITS2);
                                        rel.setProperty("scope", scope.getId());
                                    });

                        }
                    }
                });
    }

    private void print(String string, Object... args) {
        System.out.println(String.format(string, args));
    }

    private boolean matchNode(NodeMatch queryNode, Node targetNode) {

        // check labels & properties
        if (!queryNode.getLabels().stream().allMatch(targetNode::hasLabel))
            return false;

        boolean match = queryNode.getProperties().stream()
                .allMatch(objectPropertyPair ->
                        targetNode.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                                && targetNode.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));


        if (debug && match) {
            print("matchNode: matching %s with %s", queryNode, targetNode);
            print("queryNode: %s", queryNode.getProperties());
            print("targetNode: %s", targetNode.getAllProperties());
        }

        return match;
    }

    /**
     * Creates {@code FITS1} and {@code FITS2} {@link Relationship} between all matching
     * context Nodes and systems that fit according to the {@code s1Query} and
     * {@code s2Query} properties.
     * Also adds a property to the FITS Relationship to indicate which scope it
     * is contained in.
     */
    void createAllFits() {

        db.getAllNodes().stream().filter(node -> node.hasLabel(Components.SCOPE))
                .forEach(this::createFitsInScope);
    }

    /**
     * Adds {@link Label} to all context Nodes that have at least one distinct pair of
     * FITS relationships.
     */
    void labelAllReady() {

        db.getAllNodes().stream().filter(node -> node.hasLabel(Components.SCOPE))
                .forEach(this::labelReadyInScope);
    }

    /**
     * Adds {@link Label} to all context Nodes that have at least one distinct pair of
     * FITS relationships within the given scope.
     * @param scope scope in which to create the Labels
     */
    void labelReadyInScope(Node scope) {

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> contexts = scHandler.getContextsInScope(scope);
        Stream<Node> nodes = scHandler.getAllSystemsInScope(scope);

        HashSet<Node> nodesInScope  = nodes.collect(Collectors.toCollection(HashSet::new));

        Stream<Node> readyContexts = contexts.filter(context -> {
            ResourceIterator<Relationship> rels1 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS1).iterator();
            ResourceIterator<Relationship> rels2 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS2).iterator();

            HashSet<Node> set1 = new HashSet<>();
            HashSet<Node> set2 = new HashSet<>();

            rels1.forEachRemaining(rel1 -> set1.add(rel1.getEndNode()));
            rels2.forEachRemaining(rel2 -> set2.add(rel2.getEndNode()));

            set1.addAll(set2);

            // only concerned about same nodes in both sets if there is only one node in both sets
            if (set1.size() == 1)
                return false;

            rels1 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS1).iterator();
            rels2 = (ResourceIterator<Relationship>) context.getRelationships(Components.FITS2).iterator();

            return rels1.stream().anyMatch(rel1 -> nodesInScope.contains(rel1.getEndNode()))
                    && rels2.stream().anyMatch(rel2 -> nodesInScope.contains(rel2.getEndNode()));
        });

        readyContexts.forEach(context -> {
            context.addLabel(Components.READY);
            if (context.hasProperty(Components.readyContextScopeID)) {
                long[] ids = (long[]) context.getProperty(Components.readyContextScopeID);
                if (!ArrayUtils.contains(ids, scope.getId())) {
                    long[] newIds = Arrays.copyOf(ids, ids.length + 1);
                    newIds[ids.length] = scope.getId();
                    context.setProperty(Components.readyContextScopeID, newIds);
                }
            } else {
                context.setProperty(Components.readyContextScopeID, new long[]{scope.getId()});
            }
        });

    }
}
