package graphEngine;

import GraphComponents.Function;
import GraphComponents.Triplet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

/**
 * Created by pete on 06/07/17.
 */
public class Compute {

    private static int maxInteractions = 5;

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.compute", mode = Mode.SCHEMA)
    @Description("")
    public void compute() {

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
            Function.valueOf((String) selectedTriplet.context.getProperty("function")).compute(selectedTriplet);

            ReadyQueue.populateQueue(db, log);
        }


    }

}
