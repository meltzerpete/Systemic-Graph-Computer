package originalGraphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import originalGraphComponents.Function;

/**
 * Created by pete on 06/07/17.
 */
public class Functions {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "originalGraphEngine.functions", mode= Mode.SCHEMA)
    @Description("")
    public void functions() {
        ResourceIterable<Node> nodes = db.getAllNodes();
        nodes.forEach(node -> {
            Function.valueOf((String) node.getProperty("function")).compute(null);
        });
    }
}
