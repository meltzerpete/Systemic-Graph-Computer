package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Computer {

    GraphDatabaseService db;

    Log log;

    List<Result> output;

    Computer(GraphDatabaseService db, Log log) {
        this.db = db;
        this.log = log;
        this.output = new LinkedList<>();
    }

    void preProcess() {
        //TODO preProcess()
    }

    void compute(int maxInteractions) {
        //TODO compute()
    }

}
