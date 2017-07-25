package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.execute", mode = Mode.SCHEMA)
    public void execute(@Name("Max no. of interactions") long maxInteractions) {

        Computer SC = new Computer(db);
        SC.compute((int) maxInteractions);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.preProcess", mode = Mode.SCHEMA)
    public void preProcess() {

        Computer SC = new Computer(db);
        SC.preProcess();

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadMany", mode = Mode.SCHEMA)
    public void loadMany(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgram);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadManyQuery", mode = Mode.SCHEMA)
    public void loadManyQuery(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

        //TODO deal with return
    }

}
