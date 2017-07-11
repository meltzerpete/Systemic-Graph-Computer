package originalGraphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by pete on 06/07/17.
 */
public class MatchingWithProperties {

//    @Context
//    public GraphDatabaseService db;
//
//    @Context
//    public Log log;

    static StopWatch localTimer = new StopWatch();
    static StopWatch globalTimer = new StopWatch();

//    @Procedure(value = "originalGraphEngine.findMatchesWithProperties", mode = Mode.SCHEMA)
//    @Description("")
    public static void findMatchesWithProperties(GraphDatabaseService db, Log log) {

//        db.execute(TestGraphQueries.systemsWithShapeProperties);

        System.out.println("Entering findMatchesWithProperties method");

        Label contextLabel = Label.label("Context");
        Label scopeLabel = Label.label("Scope");
        RelationshipType contains = RelationshipType.withName("CONTAINS");
        RelationshipType fits1 = RelationshipType.withName("FITS_1");
        RelationshipType fits2 = RelationshipType.withName("FITS_2");

        ResourceIterator<Node> scopes = db.findNodes(scopeLabel);

        // in every scope
        globalTimer.reset();
        globalTimer.start();
        scopes.forEachRemaining((Node scope) -> {

            List<Node> childSystems = new LinkedList<>();
            List<Node> contexts = new LinkedList<>();

            localTimer.reset();
            localTimer.start();

            // create lists of all child systems and all contexts of the current scope
            scope.getRelationships(contains, Direction.OUTGOING).forEach(relationship -> {

                Node endNode = relationship.getEndNode();
                childSystems.add(endNode);
                if (endNode.hasLabel(contextLabel))
                    contexts.add(endNode);

            });

            // MATCHES
            StringBuilder matchesString = new StringBuilder();

            contexts.forEach(context -> childSystems.forEach(system -> {

                String[] labelKeys = {"s1Labels", "s2Labels"};
                Map<String, Object> contextProperties = context.getProperties(labelKeys);

                RelationshipType[] fitsRelationships = {fits1, fits2};

                for (int i = 0; i < contextProperties.size(); i++) {

                    String[] labels = (String[]) contextProperties.get(labelKeys[i]);

                    boolean labelsMatch = false;

                    if (labels.length > 0) {
                        for (String label : labels) {
                            if (system.hasLabel(Label.label(label))) {
                                labelsMatch = true;
                            } else {
                                labelsMatch = false;
                                break;
                            }
                        }
                    }

                    if (labelsMatch) {
                        context.createRelationshipTo(system, fitsRelationships[i]);
                        matchesString.append("Linking: (" +
                                context.getProperty("name") + ")-[:FITS_" + (i+1) + "]->(" +
                                system.getProperty("name") + ")\n");
                    }

                }

            }));

            localTimer.stop();
            globalTimer.suspend();

            System.out.println("===================================================================");
            System.out.println("Scope: " + scope.getAllProperties());
            System.out.println("===================================================================");
            System.out.println("Contexts:");
            contexts.forEach(childNode -> System.out.println(childNode.getProperty("name")));
            System.out.println("-------------------------------------------------------------------");
            System.out.println("All child systems:");
            childSystems.forEach(childNode -> System.out.println(childNode.getProperty("name")));
            System.out.println("-------------------------------------------------------------------");
            System.out.println("Matches:");
            if (matchesString.length() > 0) {
                matchesString.delete(matchesString.length() - 1, matchesString.length());
            }
            else {
                matchesString.append("None");
            }
            System.out.println(matchesString);
            System.out.println("-------------------------------------------------------------------");
            System.out.println("Time taken: " + String.format("%,d x 10e-9 s", localTimer.getNanoTime()));
            System.out.println("-------------------------------------------------------------------\n");

            globalTimer.resume();
        });

        globalTimer.stop();
        System.out.println("Total time taken: " + String.format("%,d x 10e-9 s", globalTimer.getNanoTime()));
    }

}
