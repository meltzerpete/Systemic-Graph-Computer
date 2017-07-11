package originalGraphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.logging.Log;
import originalGraphComponents.Components;

import java.util.HashSet;

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

//    @Procedure(value = "originalGraphEngine.indexReady", mode= Mode.SCHEMA)
//    @Description("")
    public static void indexREADY(GraphDatabaseService db, Log log) {

        System.out.println("Entering indexReady method");

        System.out.println("\n\n==============================================================");

        readyIndex = db.index().forNodes("READY");

        Label contextLabel = Label.label("Context");
        Label readyLabel = Label.label("Ready");

        timer.reset();
        timer.start();

        ResourceIterator<Node> allContexts = db.findNodes(contextLabel);


        for (ResourceIterator<Node> it = allContexts; it.hasNext(); ) {
            Node context = it.next();

            if (context.hasRelationship(Direction.OUTGOING, Components.fits1)
                    && context.hasRelationship(Direction.OUTGOING, Components.fits2)) {

                HashSet<Node> s1s = new HashSet<>();
                HashSet<Node> s2s = new HashSet<>();

                context.getRelationships(Components.fits1, Direction.OUTGOING).forEach(relationship ->
                        s1s.add(relationship.getEndNode()));

                context.getRelationships(Components.fits2, Direction.OUTGOING).forEach(relationship ->
                        s2s.add(relationship.getEndNode()));

                if (s1s.size() != 1 || !s1s.containsAll(s2s)) {
                    context.addLabel(readyLabel);
                }
            }
        }

        timer.stop();
        System.out.println("Time to index the ready contexts: " + timer.getNanoTime());

        System.out.println("==============================================================");

    }

}
