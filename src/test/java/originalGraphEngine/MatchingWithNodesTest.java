package originalGraphEngine;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by pete on 06/07/17.
 */
public class MatchingWithNodesTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(MatchingWithNodes.class);

    @Test
    public void shouldFindMatchingSystems() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {

            int nTrials = 3;

            for (int i = 0; i < nTrials; i++) {
                System.out.println("Iteration: " + i);
                System.out.println("############");
                session.run("CALL originalGraphEngine.findMatchesWithNodes");
                session.run("MATCH (n) DETACH DELETE n");
                System.out.println("\n");
            }

        }
    }

}