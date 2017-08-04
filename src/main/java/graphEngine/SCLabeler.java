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

            if (debug) print("Context: %s", context.getProperty("name"));

            // start here
            if (debug) System.out.println("\nS1\n");


            if (comp.withCypher) {

                if (context.hasProperty(Components.s1Query)) {

                    String queryString = (String) context.getProperty(Components.s1Query);
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

                if (context.hasProperty(Components.s2Query)) {

                    String queryString = (String) context.getProperty(Components.s2Query);
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
            } else {
                // compiled query matching
                Stream<Node>  targetNodes = scHandler.getAllSystemsInScope(scope)
                        .filter(node -> !node.equals(context));

                if (context.hasProperty(Components.s1Query)) {

                    String queryString = (String) context.getProperty(Components.s1Query);
                    Vertex queryGraph = comp.getMatchingGraph(queryString);

                    targetNodes
                            .filter(target -> !relationshipExists(context, target, Components.FITS1))
                            .filter(target -> {
                                if (debug) print("=== %s == %s ===", queryGraph.name, target.getProperty("name"));
                                boolean match = recursiveMatch(queryGraph, target);
                                if (debug) {
                                    if (match) {
                                        print("========MATCH========");
                                    } else {
                                        print("=======NOMATCH=======");
                                    }
                                }
                                return match;
                            })
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
                            .filter(target -> {
                                if (comp.withCypher) {
                                    return db.execute(queryBuilder(target, queryString)).hasNext();
                                } else {
                                    if (debug) print("=== %s == %s ===", queryGraph.name, target.getProperty("name"));
                                    boolean match = recursiveMatch(queryGraph, target);
                                    if (debug) {
                                        if (match) {
                                            print("========MATCH========");
                                        } else {
                                            print("=======NOMATCH=======");
                                        }
                                    }
                                    return match;
                                }
                            })
                            .forEach(target -> {
                                Relationship rel = context.createRelationshipTo(target, Components.FITS2);
                                rel.setProperty("scope", scope.getId());
                            });
                }
            }


        });
    }

    String queryBuilder(Node target, String queryString) {
//        System.out.println("Checking " + context.getProperty("name") + " against " + other.getProperty("name"));

        String returnString =
                "START n=node(" +
                        target.getId() +
                        ") MATCH " +
                        queryString +
                        " RETURN DISTINCT n LIMIT 1";
//        System.out.println(queryString);
        return returnString;
    }

    /**
     * Checks if the graph starting at the given {@link Vertex} is a subgraph of the graph
     * starting at the given {@link Node}. For a match, all Labels, Properties, RelationshipTypes,
     * and Relationship Directions must match.
     * @param vertex internal representation of graph compiled from query by {@link queryCompiler.Compiler}
     * @param target {@code Node} to check for a match
     * @return {@code true} if match
     */
    @Deprecated
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

        boolean result = recursiveMatch(Iterators.asList(outgoingEdges), Iterators.asList(targetOutRels), vertex, target, new HashMap<>());

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
    @Deprecated
    private boolean recursiveMatch(List<Edge> vQueue, List<Relationship> chQueue, Vertex currentVertex, Node currentNode, Map<Vertex, Node>visitedNodes) {

        // get all perms of chQueue
        // for each perm try to find complete match
        // remove nodes as they are matched

        return generatePerm(chQueue).stream().anyMatch(rQueue -> {

            if (debug) {
                System.out.println(currentVertex.name + ": Current perm: " + rQueue);
                System.out.println("Seen: ");
                visitedNodes.forEach((vertex1, node1) ->
                        System.out.println(vertex1.name + " : " + node1.getProperty("name")));
            }

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

                    // if node is already visited it should have the corresponding vertex
                    if (debug) {
                        System.out.println("Visited:");
                        visitedNodes.forEach((vertex1, node1) ->
                                System.out.println(vertex1.name + " : " + node1.getProperty("name")));
                    }

                    if (visitedNodes.containsKey(vertex)) {
                        if (debug) System.out.println("Vertex seen before");
                        if (!visitedNodes.get(vertex).equals(node)) {
                            // not the correct node for this vertex
                            if (debug) System.out.println("node does not match previously visited node, return false");
                            return false;
                        }
                    // check node's properties/labels
                    } else if (!matchNode(vertex, node)) {
                        if (debug) System.out.println("node does not match properties/labels, returning false");
                        return false;
                    }

                    if (vertex.getEdges().isEmpty()) {
                        if (debug) System.out.println(String.format("%s: return true", currentVertex.name));
                        visitedRelationship.add(relationship);
//                        visitedNodes.put(vertex, node);
                        return true;
                    } else {
                        Iterator<Edge> childEdges = vertex.getEdges().iterator();
                        Iterator<Relationship> childRelationships = node.getRelationships().iterator();

//                        visitedNodes.put(vertex, node);
                        boolean childPathMatches =
                                recursiveMatch(Iterators.asList(childEdges), Iterators.asList(childRelationships), vertex, node, visitedNodes);

                        if (debug) System.out.println(String.format(
                                "%s: Path from %s to bottom and from %s to target bottom match: %s",
                                currentVertex.name, vertex.name, node.getProperty("name"), childPathMatches
                        ));

                        if (childPathMatches) {
                            visitedRelationship.add(relationship);
//                            visitedNodes.put(vertex, node);
                            return true;
                        } else {
//                            visitedNodes.remove(vertex, node);
                            return false;
                        }
                    }


                });

                if (debug) System.out.println(String.format("%s: foundMatchingPath: %s", currentVertex.name, foundMatchingPath));

                return foundMatchingPath;

            });

            if (debug) {
                System.out.println(String.format("%s: foundCompleteMatch: %s", currentVertex.name, foundCompleteMatch));
                if (foundCompleteMatch) {
                    System.out.println("Path contains:");
                    visitedRelationship.forEach(System.out::println);
                    visitedNodes.forEach((vertex, node) -> System.out.println(vertex.name + " -- " + node.getProperty("name")));
                }
            }

            return foundCompleteMatch;

        });


    }

    private boolean recursiveMatch(Vertex vertex, Node node) {
        return recursiveMatch(vertex, node, new HashMap<>(), new HashSet<>(), new HashSet<>(), 0);
    }

    private boolean recursiveMatch(Vertex currentVertex, Node currentNode, Map<Vertex,Node> visitedVertices, Set<Node> visitedNodes, Set<Relationship> visitedRelationships, int stack) {
        // check currentVertex seen
            // if seen - check corresponding node
            // if not correct fail
        // ELSE check currentNode seen
            // if seen node is seen fail

        if (debug) print("(%d) rMatch: at %s :: %s ----------------------------------------------", stack, currentVertex.name, currentNode.getProperty("name"));
        if (debug) print("Visited vertices:");
        if (debug) visitedVertices.forEach((vertex, node) -> print("%s :: %s", vertex.name, node.getProperty("name")));

        if (visitedVertices.containsKey(currentVertex)) {
            if (visitedVertices.get(currentVertex).equals(currentNode)) {
                if (debug) print("%s :: %s seen v before - match: true", currentVertex.name, currentNode.getProperty("name"));
                return true;
            } else {
                if (debug) print("%s :: %s seen v before - match: false", currentVertex.name, currentNode.getProperty("name"));
                return false;
            }
        } else if (visitedNodes.contains(currentNode)) {
            if (debug) print("%s :: %s seen n before - match: false", currentVertex.name, currentNode.getProperty("name"));
            return false;
        }

        // check node properties/labels
        if (matchNode(currentVertex, currentNode)) {
            if (debug) print("%s :: %s matchNode - match: true (continue)", currentVertex.name, currentNode.getProperty("name"));
        } else {
            if (debug) print("%s :: %s matchNode - match: false (return)", currentVertex.name, currentNode.getProperty("name"));
            return false;
        }

        visitedVertices.put(currentVertex, currentNode);
        visitedNodes.add(currentNode);

        if (currentVertex.getEdges().size() == 0) {
            if (debug) print("%s :: %s noEdges - match: true (return)", currentVertex.name, currentNode.getProperty("name"));
            return true;
        }

        // check number of target relationships >= no of query edges

        Stream<Relationship> allRelList =
                Iterators.stream(currentNode.getRelationships().iterator());

        List<Relationship> relList =
                Iterators.asList(allRelList
                        .filter(relationship ->
                                currentVertex.getEdges().stream()
                                        .anyMatch(edge -> matchRelationship(edge, relationship)
                                                && matchNode(edge.getNext(), relationship.getOtherNode(currentNode))))
                        .limit(8)
                        .iterator());

        if (currentVertex.getEdges().size() > relList.size()) {
            // will not be able to match since not enough target relationships
            if (debug) print("%s has more edges than %s - match: false (return)", currentVertex.name, currentNode.getProperty("name"));
            visitedVertices.remove(currentVertex, currentNode);
            visitedNodes.remove(currentNode);
            return false;
        }

        // check depth of target graph is >= query

        if (currentVertex.getDepth() > getDepth(currentNode)) {
            // will not be able to match since target not deep enough
            if (debug) print("%s is deeper than %s - match: false (return)", currentVertex.name, currentNode.getProperty("name"));
            visitedVertices.remove(currentVertex, currentNode);
            visitedNodes.remove(currentNode);
            return false;
        }

//        System.out.println(Permutations.of(relList).count());

        boolean matchToEndOuter = Permutations.of(currentVertex.getEdges()).anyMatch(edges -> {

            boolean matchToEndInner = Permutations.of(relList).anyMatch(perm -> {

                if (debug) {
                    System.out.print("Current perm from (" + currentNode.getProperty("name") + "): ");
                    perm.forEach(relationship -> System.out.print("->(" + relationship.getOtherNode(currentNode).getProperty("name") + "), "));
                    System.out.print("\n");
                }

                Set<Node> tempNodes = new HashSet<>(visitedNodes);
                Map<Vertex, Node> tempVertices = new HashMap<>(visitedVertices);
                Set<Relationship> tempRels = new HashSet<>(visitedRelationships);

                boolean setOfEdges = edges.stream().allMatch(edge -> {

                    // relationships.(filter seen) -> anyMatch
                    boolean singleEdge = perm.stream()
                            .filter(relationship -> !visitedRelationships.contains(relationship))
                            .anyMatch(relationship -> {

                                // add to seen relationships - MUST REMOVE IF RETURNING FALSE
                                visitedRelationships.add(relationship);

                                if (debug) print("%s :: %s current edge/rel: ->(%s), ->(%s)",
                                        currentVertex.name,
                                        currentNode.getProperty("name"),
                                        edge.getNext().name,
                                        relationship.getOtherNode(currentNode).getProperty("name"));

                                // check edge / relationship direction
                                if (edge.getDirection() == Direction.INCOMING
                                        && currentNode.equals(relationship.getStartNode())) {
                                    if (debug) print("%s :: %s edgeDirection - false (return)", currentVertex.name, currentNode.getProperty("name"));
                                    visitedVertices.remove(currentVertex, currentNode);
                                    visitedNodes.remove(currentNode);
                                    visitedRelationships.remove(relationship);
                                    return false;
                                } else if (edge.getDirection() == Direction.OUTGOING
                                        && currentNode.equals(relationship.getEndNode())) {
                                    if (debug) print("%s :: %s edgeDirection - false (return)", currentVertex.name, currentNode.getProperty("name"));
                                    visitedNodes.remove(currentNode);
                                    visitedVertices.remove(currentVertex, currentNode);
                                    visitedRelationships.remove(relationship);
                                    return false;
                                }
                                //otherwise do not care about relationship direction

                                // check edge direction/properties/type
                                boolean relationshipMatches = matchRelationship(edge, relationship);

                                if (relationshipMatches) {
                                    // continue
                                    if (debug) print("%s :: %s relationshipMatches - %s (continue)", currentVertex.name, currentNode.getProperty("name"), relationshipMatches);
                                } else {
                                    if (debug) print("%s :: %s relationshipMatches - %s (return)", currentVertex.name, currentNode.getProperty("name"), relationshipMatches);
                                    visitedRelationships.remove(relationship);
                                    visitedVertices.remove(currentVertex, currentNode);
                                    visitedNodes.remove(currentNode);
                                    return false;
                                }

                                // recurse for each endNode
                                Vertex v = edge.getNext();
                                Node n = relationship.getOtherNode(currentNode);

                                boolean toEndNode = recursiveMatch(v, n, visitedVertices, visitedNodes, visitedRelationships, stack + 1);
                                if (debug) print("(%d)", stack);

                                if (toEndNode) {
                                    if (debug) print("%s :: %s toEndNode - %s (return)", currentVertex.name, currentNode.getProperty("name"), toEndNode);
                                    return true;
                                } else {
                                    if (debug) print("%s :: %s toEndNode - %s (return)", currentVertex.name, currentNode.getProperty("name"), toEndNode);
                                    visitedVertices.remove(currentVertex, currentNode);
                                    visitedRelationships.remove(relationship);
                                    visitedNodes.remove(currentNode);
                                    return false;
                                }

                                //TODO
                            });

                    if (debug) print("%s :: %s singleEdge - %s (return)", currentVertex.name, currentNode.getProperty("name"), singleEdge);
                    return singleEdge;
                });

                if (!setOfEdges) {
                    visitedNodes.clear();
                    visitedVertices.clear();
                    visitedRelationships.clear();
                    visitedNodes.addAll(tempNodes);
                    visitedVertices.putAll(tempVertices);
                    visitedRelationships.addAll(tempRels);
                }


                //TODO
                if (debug) print("%s :: %s setOfEdges - %s (return)", currentVertex.name, currentNode.getProperty("name"), setOfEdges);

                return setOfEdges;

            });

            // remove vertex and node from visited if not a match
            if (!matchToEndInner) {
                visitedVertices.remove(currentVertex, currentNode);
                visitedNodes.remove(currentNode);
            }
            return matchToEndInner;
        });

        if (debug) print("%s :: %s matchToEndOuter - %s (return)", currentVertex.name, currentNode.getProperty("name"), matchToEndOuter);

        return matchToEndOuter;
    }

    /**
     * Generates all permutations of a list of Relationships. Code taken from:
     * https://stackoverflow.com/questions/10305153/generating-all-possible-permutations-of-a-list-recursively
     * DaveFar
     */
    private static List<List<Relationship>> generatePerm(List<Relationship> original) {
        if (original.size() == 0) {
            List<List<Relationship>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        Relationship firstElement = original.remove(0);
        List<List<Relationship>> returnValue = new ArrayList<>();
        List<List<Relationship>> permutations = generatePerm(original);
        for (List<Relationship> smallerPermutated : permutations) {
            for (int index=0; index <= smallerPermutated.size(); index++) {
                List<Relationship> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }
    
    private static List<List<Edge>> generatePerm2(List<Edge> original) {
        if (original.size() == 0) {
            List<List<Edge>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        Edge firstElement = original.remove(0);
        List<List<Edge>> returnValue = new ArrayList<>();
        List<List<Edge>> permutations = generatePerm2(original);
        for (List<Edge> smallerPermutated : permutations) {
            for (int index=0; index <= smallerPermutated.size(); index++) {
                List<Edge> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    private void print(String string, Object... args) {
        System.out.println(String.format(string, args));
    }

    private boolean matchNode(Vertex vertex, Node node) {

        // check labels & properties
        if (vertex.getLabels().size() > 0 && !vertex.getLabels().stream().allMatch(node::hasLabel))
            return false;

        boolean match = vertex.getProperties().stream()
                    .allMatch(objectPropertyPair ->
                        node.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                    && node.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));


        if (debug && match) {
            print("matchNode: matching %s with %s", vertex.name, node.getProperty("name"));
            print("vertex: %s", vertex.getProperties());
            print("node: %s", node.getAllProperties());
        }

        return match;
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

        // check properties - if stream of edge properties is empty returns true
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
