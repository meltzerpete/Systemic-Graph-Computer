package originalGraphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

/**
 * Created by pete on 05/07/17.
 */
public class EditNode {

    private static StopWatch timer = new StopWatch();

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "originalGraphEngine.editNode", mode= Mode.WRITE)
    @Description("")
    public void editNode() {

        System.out.println("\n---------------------------------------------\n");

        // get a node to edit
        ResourceIterator<Node> matches = db.findNodes(Label.label("SCSystem"), "data", 7);

        int count = 0;
        matches.forEachRemaining(match -> {
            System.out.println("editNode(): Match : " + count);
            System.out.println("editNode(): node properties (before) = " + match.getAllProperties());
            match.setProperty("data", 0);
            System.out.println("editNode(): node properties (after) = " + match.getAllProperties());
            System.out.println("\t---");
        });

        System.out.println("\n---------------------------------------------\n");

    }

}
