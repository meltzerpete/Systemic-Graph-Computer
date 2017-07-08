package graphEngine;

import GraphComponents.TestGraphQueries;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.*;

/**
 * Created by pete on 06/07/17.
 */
public class Compute {

    public static List<Node> readyQueue;

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

        // while READY systems still exist
        Label readyLabel = Label.label("Ready");
        RelationshipType fits1 = RelationshipType.withName("FITS_1");
        RelationshipType fits2 = RelationshipType.withName("FITS_2");
        
        readyQueue = new ArrayList<>();
        db.findNodes(readyLabel).forEachRemaining(node -> readyQueue.add(node));

        while (readyQueue.size() > 0) {

            // get random context from the ready queue
            Node readyContext = readyQueue.remove((int) (Math.random() * readyQueue.size()));
            System.out.println(String.format("Selected %s as context.", readyContext.getAllProperties()));

            // select random pair of systems for interaction
            Relationship[] s1CandidateRelationships = Iterables.asArray(Relationship.class,
                    readyContext.getRelationships(fits1, Direction.OUTGOING));
            Node s1 = s1CandidateRelationships[(int) (Math.random() * s1CandidateRelationships.length)].getEndNode();

            Relationship[] s2CandidateRelationships = Iterables.asArray(Relationship.class,
                    readyContext.getRelationships(fits2, Direction.OUTGOING));
            Node s2 = s2CandidateRelationships[(int) (Math.random() * s2CandidateRelationships.length)].getEndNode();

            System.out.println(String.format("Selected %s and %s for interaction.", s1.getAllProperties(), s2.getAllProperties()));

            // TODO change ready queue to queue of triplets - maybe identify while indexing/finding matches
        }
    }

    static class Counter {
        static int count = 0;
        static int getAndIncrement() {
            return count++;
        }
    }

}
