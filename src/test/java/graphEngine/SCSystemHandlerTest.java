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
public class SCSystemHandlerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(TestProcedures.class);

    @Test
    public void getAllSystemsInScope() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.SCSystemHandlerTest(0)");
        }
    }

    @Test
    public void getContextsInScope() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.SCSystemHandlerTest(1)");

        }
    }

    @Test
    public void getRandomEndNode() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.SCSystemHandlerTest(2)");

        }
    }

    @Test
    public void getRandomNode() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.SCSystemHandlerTest(3)");

        }
    }

    @Test
    public void getRandomReady() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.SCSystemHandlerTest(4)");

        }
    }}