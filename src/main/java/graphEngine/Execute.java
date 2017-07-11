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

        Computer SC = new Computer(db, log);
        SC.preProcess();
        SC.compute((int) maxInteractions);

        //TODO deal with return
    }
}
