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
 */
abstract class SCLabeler {

    private Computer comp;
    private GraphDatabaseService db;
    public boolean debug = false;

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

    private boolean recursiveMatch(List<Edge> vQueue, List<Relationship> chQueue, Vertex currentVertex, Node currentNode) {

        // get all perms of chQueue
        // for each perm try to find complete match
        // remove nodes as they are matched
        
        return generatePerm(chQueue).stream().anyMatch(rQueue -> {

            if (debug) System.out.println("Current perm: " + rQueue);

            Set<Relationship> available = Iterators.asSet(rQueue.iterator());

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
                    if (!available.contains(relationship)) {
                        if (debug) System.out.println("Relationship already used in path, returning false");
                        return false;
                    }
                    available.remove(relationship);

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
                        // put back relationship as not used
                        available.add(relationship);
                        return false;
                    }

                    if (vertex.getEdges().isEmpty()) {
                        if (debug) System.out.println(String.format("%s: return true", currentVertex.name));
                        return true;
                    }

                    Stream<Edge> childEdges = vertex.getEdges().stream().filter(edge1 -> edge1.getDirection() == Direction.OUTGOING);
                    Stream<Relationship> childRelationships = Iterators.stream(node.getRelationships(Direction.OUTGOING).iterator());

                    boolean childPathMatches =
                            recursiveMatch(Iterators.asList(childEdges.iterator()), Iterators.asList(childRelationships.iterator()), vertex, node);

                    if (debug) System.out.println(String.format(
                            "%s: Path from %s to bottom and from %s to target bottom match: %s",
                            currentVertex.name, vertex.name, node.getProperty("name"), childPathMatches
                    ));

                    if (childPathMatches) return true;
                    else {
                        // put back relationship as not used
                        available.add(relationship);
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
                        node.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null);
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
        return edge.getProperties().stream()
                    .allMatch(objectPropertyPair ->
                        relationship.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null);
    }

    private int getDepth(Node n) { return xGetDepth(n, new HashSet<>()); }

    private int xGetDepth(Node node, Set<Node> seen) {

        if (seen.contains(node)) return 0;

        seen.add(node);

        ResourceIterator<Relationship> rels = (ResourceIterator<Relationship>) node.getRelationships(Direction.OUTGOING).iterator();

        return rels.stream()
                .map(Relationship::getEndNode)
                .map(node1 -> xGetDepth(node1, seen) + 1)
                .reduce(0, (n, m) -> n > m ? n : m);
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
