package graphEngine;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import queryCompiler.Edge;
import queryCompiler.Vertex;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
abstract class SCLabeler {

    private Computer comp;
    private GraphDatabaseService db;

    SCLabeler(Computer comp, GraphDatabaseService db) {
        this.comp = comp;
        this.db = db;
    }

    private boolean relationshipExists(Node from, Node to, RelationshipType relationshipType) {
        ResourceIterator<Relationship> rels =
                (ResourceIterator<Relationship>) from.getRelationships(relationshipType, Direction.OUTGOING).iterator();
        return rels.stream()
                .anyMatch(rel -> rel.getEndNode().equals(to));
    }

    void labelFitsInScope(Node scope) {
        //TODO can add extra matching conditions here - OR/AND?

        SCSystemHandler scHandler = comp.getHandler();
        Stream<Node> containedContexts = scHandler.getContextsInScope(scope);
        containedContexts.forEach(context -> {

            // start here
            System.out.println("\nS1\n");


            Stream<Node>  targetNodes = scHandler.getAllSystemsInScope(scope)
                                            .filter(node -> !node.equals(context));

            if (context.hasProperty(Components.s1Query)) {

                String queryString = (String) context.getProperty(Components.s1Query);
                Vertex queryGraph = comp.getMatchingGraph(queryString);

                targetNodes
                        .filter(target -> !relationshipExists(context, target, Components.FITS1))
                        .filter(target -> matchGraph(queryGraph, target))
                        .forEach(target -> {
                            Relationship rel = context.createRelationshipTo(target, Components.FITS1);
                            rel.setProperty("scope", scope.getId());
                        });
            }

            System.out.println("\nS2\n");

            targetNodes = scHandler.getAllSystemsInScope(scope)
                                .filter(node -> !node.equals(context));

            if (context.hasProperty(Components.s2Query)) {

                String queryString = (String) context.getProperty(Components.s2Query);
                Vertex queryGraph = comp.getMatchingGraph(queryString);

                targetNodes
                        .filter(target -> !context.equals(target))
                        .filter(target -> !relationshipExists(context, target, Components.FITS2))
                        .filter(target -> matchGraph(queryGraph, target))
                        .forEach(target -> {
                            Relationship rel = context.createRelationshipTo(target, Components.FITS2);
                            rel.setProperty("scope", scope.getId());
                        });
            }


        });
    }

    private boolean matchGraph(Vertex vertex, Node target) {

        System.out.println(String.format(
                "----- Entering matchGraph v: %s, t: %s -----",
                    vertex.name, target.getProperty("name")
        ));

        if (!matchNode(vertex, target)) {
            System.out.println(String.format(
                    "----- Exiting matchGraph v: %s, t: %s -- %s -----",
                    vertex.name, target.getProperty("name"), false
            ));
            return false;
        }

        // check edges/children

        if (vertex.getEdges().isEmpty())
            return true;

        // for outgoing edges
        Stream<Edge> outgoingEdges = vertex.getEdges().stream().filter(edge -> edge.getDirection() == Direction.OUTGOING);
        Stream<Relationship> targetOutRels = Iterators.stream(target.getRelationships(Direction.OUTGOING).iterator());

        return recursiveMatch(Iterators.asList(outgoingEdges.iterator()), Iterators.asList(targetOutRels.iterator()));

    }

    int stack = 0;
    private boolean recursiveMatch(List<Edge> vQueue, List<Relationship> chQueue) {

        boolean foundCompleteMatch = vQueue.stream().anyMatch(edge -> {

            Vertex vertex = edge.getNext();
            System.out.println(String.format("%d: Current edge: ->(%s)", stack, vertex.name));

            boolean foundMatchingPath = chQueue.stream().anyMatch(relationship -> {

                Node node = relationship.getEndNode();
                System.out.println(String.format("%d: Current relationship: ->(%s)", stack, node.getProperty("name")));

                boolean edgeToNodeMatches =
                        matchRelationship(edge, relationship)
                                && matchNode(vertex, node);

                System.out.println(String.format(
                        "%d: Path from root to %s and from target root to %s match: %s",
                        stack, vertex.name, node.getProperty("name"), edgeToNodeMatches
                ));

                if (!edgeToNodeMatches) {
                    System.out.println(String.format("%d: return false", stack));
                    return false;
                }

                if (vertex.getEdges().isEmpty())
                    return true;

                Stream<Edge> childEdges = vertex.getEdges().stream().filter(edge1 -> edge1.getDirection() == Direction.OUTGOING);
                Stream<Relationship> childRelationships = Iterators.stream(node.getRelationships(Direction.OUTGOING).iterator());

                stack++;
                boolean childPathMatches = recursiveMatch(Iterators.asList(childEdges.iterator()), Iterators.asList(childRelationships.iterator()));
                stack--;

                System.out.println(String.format(
                        "%d: Path from %s to bottom and from %s to target bottom match: %s",
                        stack, vertex.name, node.getProperty("name"), childPathMatches
                ));

                return childPathMatches;

            });

            System.out.println(String.format("%d: foundMatchingPath: %s", stack, foundMatchingPath));

            return foundMatchingPath;

        });

        System.out.println(String.format("%d: foundCompleteMatch: %s", stack, foundCompleteMatch));

        return foundCompleteMatch;
    }

    private boolean matchNode(Vertex vertex, Node node) {

        // check labels
        if (!vertex.getLabels().stream().allMatch(node::hasLabel)) return false;

        // check properties
        if (!vertex.getProperties().stream()
                .allMatch(objectPropertyPair ->
                        node.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null))
            return false;

        return true;
    }

    /**
     * Does not match relationship direction
     * @param edge
     * @param relationship
     * @return
     */
    private boolean matchRelationship(Edge edge, Relationship relationship) {

        // check type
        RelationshipType edgeType = edge.getType();
        if (edgeType != null && !edgeType.name().equals(relationship.getType().name()))
            return false;

        // check properties
        if (!edge.getProperties().stream()
                .allMatch(objectPropertyPair ->
                        relationship.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null))
            return false;

        return true;
    }

    void labelAllFits() {

        db.findNodes(Components.SCOPE).stream().forEach(this::labelFitsInScope);
    }

    void labelAllReady() {

        db.findNodes(Components.SCOPE).stream().forEach(this::labelReadyInScope);
    }

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

//        System.out.println("Labelling READY in " + scope.getProperty("key"));
        readyContexts.forEach(context -> {
            context.addLabel(Components.READY);
//            System.out.println(context.getProperty("key"));
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
