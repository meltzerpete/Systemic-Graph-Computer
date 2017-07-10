package graphEngine;

import GraphComponents.Components;
import GraphComponents.Function;
import GraphComponents.Triplet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by pete on 06/07/17.
 */
public class Compute {

    private static boolean withGraphLogging = true;

    public static List<Output> outputStream = new LinkedList<>();

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.compute", mode = Mode.SCHEMA)
    @Description("")
    public Stream<Output> compute(@Name("Maximum number of interactions") long maxInteractions) throws FileNotFoundException, UnsupportedEncodingException {

        File file = new File("graph_log");;

        PrintWriter writer = new PrintWriter(file, "UTF-8");

        logGraph(db, writer);

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

            logGraph(db, writer);

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

            logGraph(db, writer);

            MatchingWithProperties.findMatchesWithProperties(db, log);
            IndexReady.indexREADY(db, log);
            ReadyQueue.populateQueue(db, log);
        }

        Stream<Output> out = outputStream.stream();
        outputStream = new LinkedList<>();
        return out;
    }

    public static void logGraph(GraphDatabaseService db, PrintWriter writer) {
        if (withGraphLogging) {
            Result result = db.execute(
                    "MATCH (n)" +
                    "OPTIONAL MATCH (n)-[r]->(m)" +
                    "RETURN DISTINCT id(n) AS ID, labels(n) AS Labels," +
                            "properties(n) AS Properties, {r:r, n:id(m)} AS Relationships");
            result.writeAsStringTo(writer);
        }
    }

    public static class Output {

        public String out;

        public Output(String string) {
            out = string;
        }
    }

}
