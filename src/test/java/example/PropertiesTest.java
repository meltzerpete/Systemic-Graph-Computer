package example;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by pete on 01/07/17.
 */

public class PropertiesTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Properties.class);

    @Test
    public void shouldReturnLabels() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {
            System.out.println(neo4j.boltURI());
            long nodeId = session.run( "CREATE (p:User {name:'Brookreson'}) RETURN id(p)")
                    .single()
                    .get(0).asLong();
            StatementResult result = session.run("CALL example.myTest({id})", parameters("id", nodeId));
            System.out.println(neo4j.getGraphDatabaseService());
            System.out.println(neo4j.getConfig());
        }
    }
}