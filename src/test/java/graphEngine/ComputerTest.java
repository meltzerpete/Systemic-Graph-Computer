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
public class ComputerTest {

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

            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

            Computer comp = new Computer(db);

            comp.preProcess();

            String state = db.execute(TestGraphQueries.viewGraph).resultAsString();
            System.out.println(state);

            comp.compute(10);

//            state = db.execute(TestGraphQueries.viewGraph).resultAsString();
//            System.out.println(state);

            tx.success();
            tx.close();

        }
    }

    @Test
    public void executeManySC() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            for (int i = 0; i < 10; i++) {

                Transaction tx = db.beginTx();

                for (int j = 0; j < 1000; j++)
                    db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

                Computer comp = new Computer(db);

                comp.preProcess();

//            String state = db.execute(TestGraphQueries.viewGraph).resultAsString();
//            System.out.println(state);

                comp.compute(10000);

//            state = db.execute(TestGraphQueries.viewGraph).resultAsString();
//            System.out.println(state);

                tx.success();
                tx.close();

                db.execute("match (n) detach delete n");

                System.out.println();
                System.out.println();
            }

        }
    }
}