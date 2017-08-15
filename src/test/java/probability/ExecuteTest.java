package probability;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import static org.junit.Assert.*;

/**
 * Created by Pete Meltzer on 14/08/17.
 */
public class ExecuteTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Execute.class);


    @Test
    public void executeParallel() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.loadProbability(1000, 1000)");

            session.run("CALL graphEngine.runProbability(100000)");

        }
    }

}