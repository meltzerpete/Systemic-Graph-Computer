package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

/**
 * Created by pete on 01/07/17.
 */

public class TripletSelection {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.selectTriplet", mode=Mode.SCHEMA)
    @Description("")
    public void index()
    {
        System.out.println(log.isDebugEnabled());
        StopWatch timer = new StopWatch();

        // CYPHER
        // Cypher query to get triplet
        timer.start();
        Result res = db.execute("MATCH (f1:System)<-[:FITS_1]-(context:Context)-[FITS_2]->(f2:System), " +
                "(f1)<-[:CONTAINS]-(scope:Scope)-[:CONTAINS]->(f2) " +
                "RETURN context, f1, f2;");
        timer.stop();
        System.out.println("Results (FULL): (" + timer.getNanoTime() + ")");
        System.out.println(res.resultAsString());

        // CYPHER
        // Cypher query to get triplet
        timer.reset();
        timer.start();
        res = db.execute("MATCH (f1:System)<-[:FITS_1]-(context:Context)-[FITS_2]->(f2:System), " +
                "(f1)<-[:CONTAINS]-(scope:Scope)-[:CONTAINS]->(f2) " +
                "RETURN context, f1, f2;");
        timer.stop();
        System.out.println("Results (FULL): (" + timer.getNanoTime() + ")");
        System.out.println(res.resultAsString());

        // MANUAL
        timer.reset();
        timer.start();
        HashSet<Triplet> manual_matches_1 = new HashSet<>();
        for (ResourceIterator<Node> it = db.findNodes(Label.label("Context")); it.hasNext(); ) {
            Node node = it.next();

            if (node.hasRelationship(RelationshipType.withName("FITS_1"), Direction.OUTGOING)
                    && node.hasRelationship(RelationshipType.withName("FITS_2"), Direction.OUTGOING)) {

                for (Relationship rel_1 : node.getRelationships(RelationshipType.withName("FITS_1"), Direction.OUTGOING)) {
                    for (Relationship rel_2 : node.getRelationships(RelationshipType.withName("FITS_2"), Direction.OUTGOING)) {
                        for (Relationship parent_1 : rel_1.getEndNode().getRelationships(RelationshipType.withName("CONTAINS"), Direction.INCOMING)) {
                            for (Relationship parent_2 : rel_2.getEndNode().getRelationships(RelationshipType.withName("CONTAINS"), Direction.INCOMING)) {
                                if (parent_1.getStartNode().equals(parent_2.getStartNode())) {
                                    manual_matches_1.add(new Triplet(
                                            node, parent_1.getEndNode(), parent_2.getEndNode()
                                    ));
                                    System.out.println("MATCH: " + parent_1.getEndNode().getAllProperties() + ", " +
                                        parent_2.getEndNode().getAllProperties() + ", " +
                                            node.getAllProperties() + ", ");
                                }
                            }
                        }
                    }
                }

            }
        }
        timer.stop();
        System.out.println("Manual time: " + timer.getNanoTime());
        System.out.println("Matches: " + (manual_matches_1.size()));

        System.out.println();
        System.out.println();

        // MANUAL 2
        timer.reset();
        timer.start();

        HashSet<TripletSet> set_triplets = new HashSet<>();
        HashSet<Node> children = new HashSet<>();
        HashSet<Node> f1_nodes = new HashSet<>();
        HashSet<Node> f2_nodes = new HashSet<>();

        for (ResourceIterator<Node> it = db.findNodes(Label.label("Context")); it.hasNext(); ) {
            Node node = it.next();

            // get parent scope
            for (Relationship p : node.getRelationships(RelationshipType.withName("CONTAINS"), Direction.INCOMING)) {
                for (Relationship c : p.getStartNode().getRelationships(RelationshipType.withName("CONTAINS"), Direction.OUTGOING)) {
                    children.add(c.getEndNode());
                }

                // get f1 matches
                for (Relationship f1 : node.getRelationships(RelationshipType.withName("FITS_1"), Direction.OUTGOING))
                        f1_nodes.add(f1.getEndNode());

                //get f2 matches
                for (Relationship f2 : node.getRelationships(RelationshipType.withName("FITS_2"), Direction.OUTGOING))
                        f2_nodes.add(f2.getEndNode());

                f1_nodes.retainAll(children);
                f2_nodes.retainAll(children);
                if (f1_nodes.size() + f2_nodes.size() > 1) {
                    set_triplets.add(new TripletSet(node, f1_nodes, f2_nodes));
                    System.out.println("Context: " + node.getAllProperties() + "F1: " + f1_nodes + ", F2: " + f2_nodes);
                }

                f1_nodes.clear();
                f2_nodes.clear();
                children.clear();

            }

        }
        timer.stop();
        System.out.println("Set version: " + timer.getNanoTime());
        System.out.println("Matches: " + set_triplets.size());

    }

}
