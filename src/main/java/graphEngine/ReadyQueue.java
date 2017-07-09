package graphEngine;

import GraphComponents.Components;
import GraphComponents.Triplet;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by pete on 09/07/17.
 */
public class ReadyQueue {

    public static List<Node> queue = new ArrayList<>();

    public static void populateQueue(GraphDatabaseService db, Log log) {

        db.findNodes(Components.readyLabel).forEachRemaining(node -> queue.add(node));

    }

    public static Triplet getTriplet(Log log) {
        // get random context from the ready queue
        Node readyContext = ReadyQueue.queue.remove((int) (Math.random() * ReadyQueue.queue.size()));
        System.out.println(String.format("Selected %s as context.", readyContext.getAllProperties()));

        // get parent scopes
        List<TripletSet> tripletSets = new ArrayList<>();
        Iterable<Relationship> parentScopes = readyContext.getRelationships(Components.contains, Direction.INCOMING);

        // get other systems in scope
        StringBuilder results = new StringBuilder();
        parentScopes.forEach(relationship -> {
            HashSet<Node> systemsInScope = new HashSet<>();
            relationship.getStartNode().getRelationships(Components.contains, Direction.OUTGOING).forEach(containsRelationship -> {
                systemsInScope.add(containsRelationship.getEndNode());
            });

            // get f1 matches
            HashSet<Node> f1_nodes = new HashSet<>();
            for (Relationship f1 : readyContext.getRelationships(Components.fits1, Direction.OUTGOING))
                f1_nodes.add(f1.getEndNode());

            //get f2 matches
            HashSet<Node> f2_nodes = new HashSet<>();
            for (Relationship f2 : readyContext.getRelationships(Components.fits2, Direction.OUTGOING))
                f2_nodes.add(f2.getEndNode());

            f1_nodes.retainAll(systemsInScope);
            f2_nodes.retainAll(systemsInScope);
            if (f1_nodes.size() + f2_nodes.size() > 1) {
                tripletSets.add(new TripletSet(readyContext, f1_nodes, f2_nodes));
                results.append("Identified - Context: " + readyContext.getProperty("name") + ", F1: " + f1_nodes + ", F2: " + f2_nodes + "\n");
            }

        });

        System.out.println(results);

        TripletSet selectedTripletSet = tripletSets.get((int) (Math.random() * tripletSets.size()));

        Node[] s1s = Iterables.asArray(Node.class, selectedTripletSet.f1);
        Node selectedS1 = s1s[(int) (Math.random() * s1s.length)];

        Node[] s2s = Iterables.asArray(Node.class, selectedTripletSet.f2);
        Node selectedS2 = s2s[(int) (Math.random() * s2s.length)];

        Triplet selectTriplet = new Triplet(readyContext, selectedS1, selectedS2);

        System.out.println(String.format("Selected %s and %s for interaction.",
                selectTriplet.s1.getProperty("name"), selectTriplet.s2.getProperty("name")));

        return selectTriplet;
    }
}
