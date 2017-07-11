package originalGraphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.HashSet;

/**
 * Created by pete on 05/07/17.
 */
public class ID {

    private static StopWatch timer = new StopWatch();

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "originalGraphEngine.id", mode= Mode.SCHEMA)
    @Description("")
    public void id() {

        Label contextLabel = Label.label("Context");
        Label readyLabel = Label.label("Ready");
        RelationshipType s1Relation = RelationshipType.withName("FITS_1");
        RelationshipType s2Relation = RelationshipType.withName("FITS_2");

        ResourceIterator<Node> allContexts = db.findNodes(contextLabel);

        HashSet<Long> readyContextIDs = new HashSet<>();

        timer.start();
        allContexts.forEachRemaining(context -> {
            if (context.hasRelationship(Direction.OUTGOING, s1Relation, s2Relation))
                readyContextIDs.add(context.getId());
        });
        timer.stop();

        System.out.println("Time to get IDs of all ready contexts: " + timer.getNanoTime());
        System.out.println("IDs: " + readyContextIDs.toString());

    }

}
