package graphEngine;

import jdk.nashorn.internal.runtime.logging.Logger;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by pete on 05/07/17.
 */
public class Indexing {

    private static StopWatch timer = new StopWatch();
    private static Index<Node> nodeIndex;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.index", mode= Mode.SCHEMA)
    @Description("")
    public void index() {

        nodeIndex = db.index().forNodes("READY");

        Label contextLabel = Label.label("Context");
        Label readyLabel = Label.label("Ready");
        RelationshipType s1Relation = RelationshipType.withName("FITS_1");
        RelationshipType s2Relation = RelationshipType.withName("FITS_2");

        timer.start();

        ResourceIterator<Node> allContexts = db.findNodes(contextLabel);

        int index = 0;
        for (ResourceIterator<Node> it = allContexts; it.hasNext(); ) {
            Node context = it.next();

            if (context.hasRelationship(Direction.OUTGOING, s1Relation, s2Relation)) {
                context.addLabel(readyLabel);
                nodeIndex.add(context, "R", index++);
            }
        }

        timer.stop();
        System.out.println("Time to index the ready contexts: " + timer.getNanoTime());

        timer.reset();
        timer.start();
        ResourceIterator<Node> readyNodes = db.findNodes(readyLabel);
        timer.stop();

        System.out.println("Time to find ready contexts once indexed: " + timer.getNanoTime());
        readyNodes.stream().forEach(node -> {
            System.out.println(node.getAllProperties() + ", id: " + node.getId());
        });


        System.out.println(Arrays.toString(db.index().nodeIndexNames()));

        timer.reset();
        long n;
        timer.start();
        n = nodeIndex.get("R", 0).getSingle().getId();
        timer.stop();
        System.out.println("Returned by nodeIndex.get(): " + n);
        System.out.println("Time to get id based on index: " + timer.getNanoTime());
    }

}
