package graphEngine;

import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterators;
import queryCompiler.Edge;
import queryCompiler.Vertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 11/07/17.
 *
 * Handles all automatic creation of FITS Relationships and Labeling of READY Contexts.
 */
abstract class SCLabeler {

    private Computer comp;
    private GraphDatabaseService db;
    boolean debug = false;

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

            // start here
            if (debug) System.out.println("\nS1\n");


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

            if (debug) System.out.println("\nS2\n");

            targetNodes = scHandler.getAllSystemsInScope(scope)
                                .filter(node -> !node.equals(context));

            if (context.hasProperty(Components.s2Query)) {

                String queryString = (String) context.getProperty(Components.s2Query);
                Vertex queryGraph = comp.getMatchingGraph(queryString);

                targetNodes
                        .filter(target -> !relationshipExists(context, target, Components.FITS2))
                        .filter(target -> matchGraph(queryGraph, target))
                        .forEach(target -> {
                            Relationship rel = context.createRelationshipTo(target, Components.FITS2);
                            rel.setProperty("scope", scope.getId());
                        });
            }


        });
    }

    /**
     * Checks if the graph starting at the given {@link Vertex} is a subgraph of the graph
     * starting at the given {@link Node}. For a match, all Labels, Properties, RelationshipTypes,
     * and Relationship Directions must match.
     * @param vertex internal representation of graph compiled from query by {@link queryCompiler.Compiler}
     * @param target {@code Node} to check for a match
     * @return {@code true} if match
     */
    private boolean matchGraph(Vertex vertex, Node target) {

         if (debug) System.out.println(String.format(
                "\n----- Entering matchGraph v: %s, t: %s -----",
                    vertex.name, target.getProperty("name")
        ));

        if (!matchNode(vertex, target)) {
             if (debug) System.out.println(String.format(
                    "----- Exiting matchGraph v: %s, t: %s -- %s ----- (node properties/labels do not match)",
                    vertex.name, target.getProperty("name"), false
            ));
            return false;
        }

        // check depth of tree before exploring expensive graph isomorphism
        if (vertex.getDepth() > getDepth(target)) {
            if (debug) System.out.println(String.format(
                    "----- Exiting matchGraph v: %s, t: %s -- %s ----- (depth of graph does not match)",
                    vertex.name, target.getProperty("name"), false
            ));
            return false;
        }

        // check edges/children
        if (vertex.getEdges().isEmpty()) {
             if (debug) System.out.println(String.format(
                    "----- Exiting matchGraph v: %s, t: %s -- %s -----",
                    vertex.name, target.getProperty("name"), true
            ));
            return true;
        }

        Iterator<Edge> outgoingEdges = vertex.getEdges().iterator();
        Iterator<Relationship> targetOutRels = target.getRelationships(Direction.OUTGOING).iterator();

        boolean result = recursiveMatch(Iterators.asList(outgoingEdges), Iterators.asList(targetOutRels), vertex, target);

        if (debug) System.out.println(String.format(
                "----- Exiting matchGraph v: %s, t: %s -- %s -----",
                vertex.name, target.getProperty("name"), result
        ));

        return result;
    }

    /**
     * A recursive graph matching algorithm.
     * @param vQueue queue of all query edges to check from current compiled vertex
     * @param chQueue queue of target relationships to check from target node
     * @param currentVertex start vertex
     * @param currentNode start node
     * @return {@code true} if match is found
     */
    private boolean recursiveMatch(List<Edge> vQueue, List<Relationship> chQueue, Vertex currentVertex, Node currentNode) {

        // get all perms of chQueue
        // for each perm try to find complete match
        // remove nodes as they are matched
        
        return generatePerm(chQueue).stream().anyMatch(rQueue -> {

            if (debug) System.out.println("Current perm: " + rQueue);

            Set<Relationship> visitedRelationship = new HashSet<>();

            boolean foundCompleteMatch = vQueue.stream().allMatch(edge -> {

                Vertex vertex = edge.getNext();
                if (debug) System.out.println(String.format("%s: MATCHING EDGE: ->(%s)", currentVertex.name, vertex.name));

                boolean foundMatchingPath = rQueue.stream().anyMatch(relationship -> {

                    Node node = relationship.getOtherNode(currentNode);
                    if (debug) System.out.println(String.format("%s: Current relationship: ->(%s)", currentVertex.name, node.getProperty("name")));

                    // check edge / relationship direction
                    if (edge.getDirection() == Direction.INCOMING
                            && currentNode.equals(relationship.getStartNode())) {
                        if (debug) System.out.println("Relationship has wrong direction, returning false");
                        return false;
                    } else if (edge.getDirection() == Direction.OUTGOING
                            && currentNode.equals(relationship.getEndNode())) {
                        if (debug) System.out.println("Relationship has wrong direction, returning false");
                        return false;
                    }
                    //otherwise do not care about relationship direction

                    // check if edge already used in path
                    if (visitedRelationship.contains(relationship)) {
                        if (debug) System.out.println("Relationship already used in path, returning false");
                        return false;
                    }

                    // check if edge has correct type/properties
                    boolean edgeToNodeMatches =
                            matchRelationship(edge, relationship)
                                    && matchNode(vertex, node);

                    if (debug) System.out.println(String.format(
                            "%s: Path from root to %s and from target root to %s match: %s",
                            currentVertex.name, vertex.name, node.getProperty("name"), edgeToNodeMatches
                    ));

                    if (!edgeToNodeMatches) {
                        if (debug) System.out.println(String.format("%s: return false", currentVertex.name));
                        return false;
                    }

                    System.out.println(String.format("v: %s, n: %s, %s ", vertex.name, node.getProperty("name"), matchNode(vertex, node)));

                    // check node
                    if (!matchNode(vertex, node)) {
                        if (debug) System.out.println("node does not match, returning false");
                        return false;
                    }

                    if (vertex.getEdges().isEmpty()) {
                        if (debug) System.out.println(String.format("%s: return true", currentVertex.name));
                        visitedRelationship.add(relationship);
                        return true;
                    }

                    Iterator<Edge> childEdges = vertex.getEdges().iterator();
                    Iterator<Relationship> childRelationships = node.getRelationships().iterator();

                    boolean childPathMatches =
                            recursiveMatch(Iterators.asList(childEdges), Iterators.asList(childRelationships), vertex, node);

                    if (debug) System.out.println(String.format(
                            "%s: Path from %s to bottom and from %s to target bottom match: %s",
                            currentVertex.name, vertex.name, node.getProperty("name"), childPathMatches
                    ));

                    if (childPathMatches) {
                        visitedRelationship.add(relationship);
                        return true;
                    } else {
                        return false;
                    }

                });

                if (debug) System.out.println(String.format("%s: foundMatchingPath: %s", currentVertex.name, foundMatchingPath));

                return foundMatchingPath;

            });

            if (debug) System.out.println(String.format("%s: foundCompleteMatch: %s", currentVertex.name, foundCompleteMatch));

            return foundCompleteMatch;
            
        });


    }

    /**
     * Generates all permutations of a list of Relationships. Code taken from:
     * https://stackoverflow.com/questions/10305153/generating-all-possible-permutations-of-a-list-recursively
     * DaveFar
     */
    private static List<List<Relationship>> generatePerm(List<Relationship> original) {
        if (original.size() == 0) {
            List<List<Relationship>> result = new ArrayList<List<Relationship>>();
            result.add(new ArrayList<Relationship>());
            return result;
        }
        Relationship firstElement = original.remove(0);
        List<List<Relationship>> returnValue = new ArrayList<List<Relationship>>();
        List<List<Relationship>> permutations = generatePerm(original);
        for (List<Relationship> smallerPermutated : permutations) {
            for (int index=0; index <= smallerPermutated.size(); index++) {
                List<Relationship> temp = new ArrayList<Relationship>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    private boolean matchNode(Vertex vertex, Node node) {

        // check labels & properties
        return vertex.getLabels().stream().allMatch(node::hasLabel)
                && vertex.getProperties().stream()
                    .allMatch(objectPropertyPair ->
                        node.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                    && node.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
    }

    /**
     * Checks for matching {@link RelationshipType} and Properties.
     * Does not match relationship direction.
     * @param edge compiled edge
     * @param relationship target Relationship
     * @return true if match
     */
    private boolean matchRelationship(Edge edge, Relationship relationship) {

        // check type
        RelationshipType edgeType = edge.getType();
        if (edgeType != null && !edgeType.name().equals(relationship.getType().name()))
            return false;

        // check properties
        return edge.getProperties().stream()
                    .allMatch(objectPropertyPair ->
                        relationship.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                    && relationship.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
    }

    /**
     * Wrapper method to call recursive graph depth algorithm. Only calculates depth of
     * OUTGOING relationships.
     * @param n {@code Node} to calculate depth of
     * @return depth of graph from given Node
     */
    private int getDepth(Node n) { return getDepth(n, new HashSet<>()); }

    /**
     * Auxiliary function for depth
     * @param node start Node
     * @param seen set of Nodes visited so far, should be initialised with empty set
     * @return depth from given Node
     */
    private int getDepth(Node node, Set<Node> seen) {

        if (seen.contains(node)) return 0;

        seen.add(node);

        ResourceIterator<Relationship> rels = (ResourceIterator<Relationship>) node.getRelationships(Direction.OUTGOING).iterator();

        return rels.stream()
                .map(Relationship::getEndNode)
                .map(node1 -> getDepth(node1, seen) + 1)
                .reduce(0, (n, m) -> n > m ? n : m);
    }

    /**
     * Creates {@code FITS1} and {@code FITS2} {@link Relationship} between all matching
     * context Nodes and systems that fit according to the {@code s1Query} and
     * {@code s2Query} properties.
     * Also adds a property to the FITS Relationship to indicate which scope it
     * is contained in.
     */
    void createAllFits() {

        db.findNodes(Components.SCOPE).stream().forEach(this::createFitsInScope);
    }

    /**
     * Adds {@link Label} to all context Nodes that have at least one distinct pair of
     * FITS relationships.
     */
    void labelAllReady() {

        db.findNodes(Components.SCOPE).stream().forEach(this::labelReadyInScope);
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
