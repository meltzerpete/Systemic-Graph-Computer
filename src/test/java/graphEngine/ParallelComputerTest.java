package graphEngine;

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
 * Created by Pete Meltzer on 15/07/17.
 */
public class ParallelComputerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Computer.class);

    @Test
    public void executeSC() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();

            for (int i = 0; i < 1000; i++)
                db.execute(TestGraphQueries.terminatingProgram);

            tx.success();
            tx.close();

            Computer comp1 = new Computer(db);
            Computer comp2 = new Computer(db);

            System.out.println(Runtime.getRuntime().availableProcessors());

            Transaction tx2 = db.beginTx();
            comp1.preProcess();
            tx2.success();
            tx2.close();

            Thread.sleep(5000);

            (new Thread(comp1)).start();

            (new Thread(comp2)).start();




        }
    }

}