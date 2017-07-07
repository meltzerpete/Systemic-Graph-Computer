package graphEngine;

import GraphComponents.TestGraphQueries;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.graphdb.Result;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Iterator;

/**
 * Created by pete on 05/07/17.
 */
public class EditNodeTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(EditNode.class);

    @Test
    public void shouldTrackIDsOfReadyContexts() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {

            session.run(TestGraphQueries.basicSubtraction);

            StatementResult all = session.run("MATCH (a) RETURN a, a.data");
            System.out.println(all.list(record -> {
                return "\nnode: " + record.get(0) + ", data: " + record.get(1);
            }));

            System.out.println();

            StatementResult res1 = session.run("MATCH (a:SCSystem {name:'A1'})" +
                    "RETURN a, a.data");
            System.out.println(res1.list(record -> {
                return "node: " + record.get(0) + ", data: " + record.get(1);
            }));

            session.run("CALL graphEngine.editNode()");

            all = session.run("MATCH (a) RETURN a, a.data");
            System.out.println(all.list(record -> {
                return "\nnode: " + record.get(0) + ", data: " + record.get(1);
            }));

            System.out.println();

            StatementResult res2 = session.run("MATCH (a:SCSystem {name:'A1'})" +
                    "RETURN a, a.data");
            System.out.println(res2.list(record -> {
                return "node: " + record.get(0) + ", data: " + record.get(1);
            }));
        }
    }
}