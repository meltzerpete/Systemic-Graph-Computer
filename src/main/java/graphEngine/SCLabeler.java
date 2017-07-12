package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
abstract class SCLabeler {

    private Computer comp;
    private GraphDatabaseService db;
    private SCSystemHandler scHandler;

    SCLabeler(Computer comp, GraphDatabaseService db) {
        this.comp = comp;
        this.db = db;
        this.scHandler = comp.getHandler();
    }

    void labelFitsInScope(Node scope) {
        //TODO labelFitsInScope()
        Stream<Node> containedContexts = scHandler.getContextsInScope(scope);

    }

    void labelAllFits() {
        //TODO labelAllFits()
    }

    void labelAllReady() {
        //TODO labelAllReady()
    }

    void labelReadyInScope() {
        //TODO labelReadyInScope()
    }

    private boolean fits1(Node context, Node node) {
        //TODO fits1/2() checks

        //for each s1 label:
            //check label exists on node

        if (context.hasProperty("s1Labels")) {
            String[] s1Labels = (String[]) context.getProperty("s1Labels");
//            System.out.println("fits1: " + Arrays.toString(s1Labels));



        }

        return false;
    }
}
