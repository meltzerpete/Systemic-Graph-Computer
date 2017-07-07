package graphEngine;

import GraphComponents.TestGraphQueries;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;

/**
 * Created by pete on 05/07/17.
 */
public class Indexing {

    private static StopWatch timer = new StopWatch();
    private static Index<Node> readyIndex;

    public static int index;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.index", mode= Mode.SCHEMA)
    @Description("")
    public void index() {

        db.execute(TestGraphQueries.basicSubtraction);

        for (int i = 0; i < 2; i++) {

            System.out.println("\n\n==============================================================\n" +
                    "Iteration " + i + ":");

            readyIndex = db.index().forNodes("READY");

            Label contextLabel = Label.label("Context");
            Label readyLabel = Label.label("Ready");
            RelationshipType s1Relation = RelationshipType.withName("FITS_1");
            RelationshipType s2Relation = RelationshipType.withName("FITS_2");

            if (i < 1) {
                timer.reset();
                timer.start();

                ResourceIterator<Node> allContexts = db.findNodes(contextLabel);


                for (ResourceIterator<Node> it = allContexts; it.hasNext(); ) {
                    Node context = it.next();

                    if (context.hasRelationship(Direction.OUTGOING, s1Relation)
                            && context.hasRelationship(Direction.OUTGOING, s2Relation)) {
                        context.addLabel(readyLabel);
                        readyIndex.add(context, "R", index++);
                    }
                }

                timer.stop();
                System.out.println("Time to index the ready contexts: " + timer.getNanoTime());
            }

            timer.reset();
            timer.start();
            ResourceIterator<Node> readyNodes = readyIndex.get("R", "*");
            timer.stop();

            System.out.println("Time to find ready contexts once indexed: " + timer.getNanoTime());

            readyNodes.stream().forEach(node -> {
                System.out.println(node.getAllProperties() + ", id: " + node.getId());
            });

            timer.reset();
            timer.start();
            readyNodes = db.findNodes(readyLabel);
            timer.stop();

            System.out.println("Time to find ready contexts by label: " + timer.getNanoTime());

            readyNodes.stream().forEach(node -> {
                System.out.println(node.getAllProperties() + ", id: " + node.getId());
            });

            System.out.println("All indexes in system" + Arrays.toString(db.index().nodeIndexNames()));

            // change structure of graph (escape a system and rematch)
            if (i < 1) {
                RelationshipType contains = RelationshipType.withName("CONTAINS");

                Node a1 = db.findNodes(Label.label("System"), "name", "A1").next();
                Node a3 = db.findNodes(Label.label("System"), "name", "A3").next();
                Node main = db.findNodes(Label.label("System"), "name", "main").next();
                Node print = db.findNodes(Label.label("System"), "name", "print").next();
                Node mult = db.findNodes(Label.label("System"), "name", "*").next();

                a1.getSingleRelationship(contains, Direction.INCOMING).delete();
                main.createRelationshipTo(a1, contains);
                a1.getSingleRelationship(s1Relation, Direction.INCOMING).delete();
                print.createRelationshipTo(a1, s1Relation);
                mult.createRelationshipTo(a1, s1Relation);

                a3.getSingleRelationship(contains, Direction.INCOMING).delete();
                main.createRelationshipTo(a3, contains);
                a3.getSingleRelationship(s1Relation, Direction.INCOMING).delete();
                print.createRelationshipTo(a3, s2Relation);
                mult.createRelationshipTo(a3, s2Relation);

                main.addLabel(readyLabel);
                readyIndex.add(main, "R", index++);

                print.addLabel(readyLabel);
                readyIndex.add(print, "R", index++);

                ResourceIterator<Node> subEs = db.findNodes(Label.label("System"), "name", "-e");
                subEs.forEachRemaining(subE -> {
                    subE.removeLabel(readyLabel);
                    readyIndex.remove(subE);
                });

                // change data test
                a3.setProperty("data", ((long) a3.getProperty("data")) - 3);

            }
            System.out.println("==============================================================");
        }

    }

}
