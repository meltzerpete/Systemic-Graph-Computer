package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;

/**
 * Created by pete on 05/07/17.
 */
public class IndexReady {

    private static StopWatch timer = new StopWatch();
    private static Index<Node> readyIndex;

    public static int index;

//    @Context
//    public GraphDatabaseService db;
//
//    @Context
//    public Log log;

//    @Procedure(value = "graphEngine.indexReady", mode= Mode.SCHEMA)
//    @Description("")
    public static void indexREADY(GraphDatabaseService db, Log log) {

        System.out.println("Entering indexReady method");

        System.out.println("\n\n==============================================================");

        readyIndex = db.index().forNodes("READY");

        Label contextLabel = Label.label("Context");
        Label readyLabel = Label.label("Ready");
        RelationshipType s1Relation = RelationshipType.withName("FITS_1");
        RelationshipType s2Relation = RelationshipType.withName("FITS_2");

        timer.reset();
        timer.start();

        ResourceIterator<Node> allContexts = db.findNodes(contextLabel);


        for (ResourceIterator<Node> it = allContexts; it.hasNext(); ) {
            Node context = it.next();

            if (context.hasRelationship(Direction.OUTGOING, s1Relation)
                    && context.hasRelationship(Direction.OUTGOING, s2Relation)) {
                context.addLabel(readyLabel);
            }
        }

        timer.stop();
        System.out.println("Time to index the ready contexts: " + timer.getNanoTime());

        System.out.println("==============================================================");

    }

}
