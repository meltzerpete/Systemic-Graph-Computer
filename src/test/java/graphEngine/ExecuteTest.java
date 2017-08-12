package graphEngine;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class ExecuteTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Execute.class);

    @Test
    public void executeSC() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.execute(1)");

        }
    }

    @Test
    public void testRandomSelection() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.execute(1000)");

        }
    }

    @Test
    public void executeParallel() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.loadParallel(50)");

            session.run("CALL graphEngine.runParallel(10000)");
        }
    }
}