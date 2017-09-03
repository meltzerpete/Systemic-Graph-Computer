package parallel;

import graphEngine.TestGraphQueries;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;


/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ManagerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void goTest() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE)
                .toConfig());
             Session session = driver.session()) {
            
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            // create program
            int noOfDataSystems = 50;
            try (Transaction tx = db.beginTx()) {

                TestGraphQueries.knapsack(db, noOfDataSystems);
                tx.success();
            }

            Manager manager = new Manager(1000, db);
            manager.go();
        }
    }
}