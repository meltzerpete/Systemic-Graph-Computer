package originalGraphEngine;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;
import originalGraphComponents.TestGraphQueries;

/**
 * Created by pete on 05/07/17.
 */
public class IDTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ID.class);

    @Test
    public void shouldTrackIDsOfReadyContexts() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {
            session.run(TestGraphQueries.basicSubtraction);

            session.run("CALL originalGraphEngine.id()");
        }
    }
}