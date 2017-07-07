package graphEngine;

import GraphComponents.TestGraphQueries;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by pete on 01/07/17.
 */

public class TripletSelectionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(TripletSelection.class)
                    .withProcedure(EditNode.class);

    @Test
    public void shouldFindTriplets() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {
            session.run( TestGraphQueries.basicSubtraction);
            StatementResult result = session.run("CALL graphEngine.selectTriplet()");
//            StatementResult resul2 = session.run("CALL graphEngine.selectTriplet()");
//            StatementResult resul3 = session.run("CALL graphEngine.selectTriplet()");
        }
    }
}