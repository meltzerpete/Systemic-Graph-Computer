package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Computer {

    final GraphDatabaseService db;
    private List<Result> output;

    private final SCLabeler labeler;
    private final SCSystemHandler handler;

    Computer(GraphDatabaseService db) {
        this.db = db;
        this.output = new LinkedList<>();

        this.handler = new SCSystemHandler();
        this.labeler = new SCLabeler();
    }

    void preProcess() {
        //TODO preProcess()
    }

    void compute(int maxInteractions) {
        //TODO compute()
    }

    SCLabeler getLabeler() {
        return labeler;
    }

    SCSystemHandler getHandler() {
        return handler;
    }

    private class SCSystemHandler extends graphEngine.SCSystemHandler {
        SCSystemHandler() {
            super(Computer.this, db);
        }
    }

    private class SCLabeler extends graphEngine.SCLabeler {

        SCLabeler() {
            super(Computer.this, db);
        }
    }
}
