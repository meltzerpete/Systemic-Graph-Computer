package graphEngine;

import GraphComponents.TestGraphQueries;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by pete on 05/07/17.
 */
public class IndexingTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Indexing.class);

    @Test
    public void shouldIndexReadyContexts() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {
            session.run(TestGraphQueries.basicSubtraction);

            session.run("CALL graphEngine.index()");
        }
    }
}