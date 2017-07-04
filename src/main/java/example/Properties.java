package example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

/**
 * Created by pete on 01/07/17.
 */

public class Properties {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure(value = "example.myTest", mode=Mode.SCHEMA)
    @Description("")
    public void index( @Name("nodeId") long nodeId )
    {

        for (Node node : db.getAllNodes()) {
            for ( Label label : node.getLabels() )
            {
                log.info("%s: %s, ", node.getId(), label);
                System.out.println(label);

            }
        }


    }

}
