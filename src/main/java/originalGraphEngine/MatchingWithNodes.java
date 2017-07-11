package originalGraphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import originalGraphComponents.TestGraphQueries;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by pete on 06/07/17.
 */
public class MatchingWithNodes {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    static StopWatch localTimer = new StopWatch();
    static StopWatch globalTimer = new StopWatch();

    @Procedure(value = "originalGraphEngine.findMatchesWithNodes", mode = Mode.SCHEMA)
    @Description("")
    public void findMatches() {

        db.execute(TestGraphQueries.systemsWithShapeNodes);

        Label contextLabel = Label.label("Context");
        Label scopeLabel = Label.label("Scope");
        RelationshipType contains = RelationshipType.withName("CONTAINS");
        RelationshipType hasShape = RelationshipType.withName("HAS_SHAPE");
        RelationshipType s1 = RelationshipType.withName("S1");
        RelationshipType s2 = RelationshipType.withName("S2");
        RelationshipType fits1 = RelationshipType.withName("FITS_1");
        RelationshipType fits2 = RelationshipType.withName("FITS_2");

        ResourceIterator<Node> scopes = db.findNodes(scopeLabel);

        // in every scope
        globalTimer.reset();
        globalTimer.start();
        scopes.forEachRemaining((Node scope) -> {

            localTimer.reset();
            localTimer.start();

            List<SCSystemWrapper> contexts = new LinkedList<>();
            List<SCSystemWrapper> systems = new LinkedList<>();

            // create a list of all child systems of the current scope
            scope.getRelationships(contains, Direction.OUTGOING).forEach(containsRelationship -> {

                SCSystemWrapper system = new SCSystemWrapper();
                system.node = containsRelationship.getEndNode();

                // add the shape property nodes
                system.node.getRelationships(hasShape, Direction.OUTGOING)
                        .forEach(shapeRelationship -> {
                                system.shape.add(shapeRelationship.getEndNode());
                });

                // add each shape to s1
                system.node.getRelationships(s1, Direction.OUTGOING)
                        .forEach(s1Relationship ->
                                system.s1.add(s1Relationship.getEndNode()));

                // add each shape to s2;
                system.node.getRelationships(s2, Direction.OUTGOING)
                        .forEach(s2Relationship ->
                                system.s2.add(s2Relationship.getEndNode()));

                systems.add(system);

                if (system.node.hasLabel(contextLabel))
                    contexts.add(system);

            });

            // MATCHES

            StringBuilder matchesString = new StringBuilder();
            contexts.forEach(context -> {
                systems.forEach(system -> {

                    // check s1

                    if (!context.s1.isEmpty()
                            && context.s1.stream().allMatch(node ->
                                    system.shape.contains(node))) {

                        context.node.createRelationshipTo(system.node, fits1);
                        matchesString.append("Linking: (" +
                                context.node.getProperty("name") + ")-[:FITS_1]->(" +
                                system.node.getProperty("name") + ")\n");
                    }

                    // check s2
                    if (!context.s2.isEmpty()
                            && context.s2.stream().allMatch(node ->
                            system.shape.contains(node))) {

                        context.node.createRelationshipTo(system.node, fits2);
                        matchesString.append("Linking: (" +
                                context.node.getProperty("name") + ")-[:FITS_2]->(" +
                                system.node.getProperty("name") + ")\n");
                    }
                });
            });

            localTimer.stop();
            globalTimer.suspend();

            System.out.println("===================================================================");
            System.out.println("Scope: " + scope.getAllProperties());
            System.out.println("===================================================================");
            System.out.println("Contexts:");
            contexts.forEach(scSystemWrapper -> System.out.println(scSystemWrapper));
            System.out.println("-------------------------------------------------------------------");
            System.out.println("All child systems:");
            systems.forEach(scSystemWrapper -> System.out.println(scSystemWrapper));
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
            float f = 6;
            System.out.println("Time taken: " + String.format("%,d x 10e-9 s", localTimer.getNanoTime()));
            System.out.println("-------------------------------------------------------------------\n");

            globalTimer.resume();
        });

        System.out.println("Total time taken: " + String.format("%,d x 10e-9 s", globalTimer.getNanoTime()));
        globalTimer.stop();
    }

    private class SCSystemWrapper {
        private Node node;
        private List<Node> s1;
        private List<Node> s2;
        private List<Node> shape;

        {
            s1 = new LinkedList<>();
            s2 = new LinkedList<>();
            shape = new LinkedList<>();
        }

        @Override
        public String toString() {
            String out = node == null ? "null" : node.getAllProperties().toString();
            out += s1 == null ? "" : ", s1: " + s1.toString();
            out += s2 == null ? "" : ", s2: " + s2.toString();
            out += shape == null ? "" : ", shape: " + shape.toString();
            return out;
        }
    }
}
