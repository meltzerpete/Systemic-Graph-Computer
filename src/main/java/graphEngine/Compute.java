package graphEngine;

import GraphComponents.Components;
import GraphComponents.Function;
import GraphComponents.Triplet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by pete on 06/07/17.
 */
public class Compute {

    public static List<Output> outputStream = new LinkedList<>();

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.compute", mode = Mode.SCHEMA)
    @Description("")
    public Stream<Output> compute(@Name("Maximum number of interactions") long maxInteractions) {

        // create FITS relationships for all matching systems inside each scope
//        db.execute("CALL graphEngine.findMatchesWithProperties");
        MatchingWithProperties.findMatchesWithProperties(db, log);

        // index READY systems
//        db.execute("CALL graphEngine.indexReady");
        IndexReady.indexREADY(db, log);

        // populate ready queue
        ReadyQueue.populateQueue(db, log);

        // while READY systems still exist
        while (ReadyQueue.queue.size() > 0
                && maxInteractions-- > 0) {
            Triplet selectedTriplet = ReadyQueue.getTriplet(log);

            // perform function
            outputStream.add(new Output("Performing " + selectedTriplet.context.getProperty("function")
                    + " on " + selectedTriplet));
            Function.valueOf((String) selectedTriplet.context.getProperty("function")).compute(selectedTriplet);

            db.getAllNodes().forEach(node -> {
                node.removeLabel(Components.readyLabel);
            });

            db.getAllRelationships().forEach(relationship -> {
                if (relationship.isType(Components.fits1))
                    relationship.delete();
            });

            db.getAllRelationships().forEach(relationship -> {
                if (relationship.isType(Components.fits2))
                    relationship.delete();
            });

            MatchingWithProperties.findMatchesWithProperties(db, log);
            IndexReady.indexREADY(db, log);
            ReadyQueue.populateQueue(db, log);
        }

        return outputStream.stream();
    }

    public static class Output {

        public String out;

        public Output(String string) {
            out = string;
        }
    }

}
