package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import parallel.Manager;

import static graphEngine.TestGraphQueries.viewGraph;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.execute", mode = SCHEMA)
    public void execute(@Name("Max no. of interactions") long maxInteractions) {

        Computer SC = new Computer(db);
        SC.compute((int) maxInteractions);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.preProcess", mode = SCHEMA)
    public void preProcess() {

        Computer SC = new Computer(db);
        SC.preProcess();

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadMany", mode = SCHEMA)
    public void loadMany(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgram);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadManyQuery", mode = SCHEMA)
    public void loadManyQuery(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

        //TODO deal with return
    }

}
