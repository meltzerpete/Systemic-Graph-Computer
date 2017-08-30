package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import parallel.Manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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
    public void testExecute() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            int[] noOfDataSystems = {1000};

            File file = new File("sc2-knapsack.csv");
            FileWriter fwriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fwriter);

            writer.write("Knapsack Testing\n");
            writer.write("No. of Solution Systems, Trial, Time (ms)\n");

            StopWatch exeTimer = new StopWatch();

            for (int n = 0; n < noOfDataSystems.length; n++) {
                for (int t = 0; t < 5; t++) {

                    System.out.println(String.format(
                            "\nNo. of solution systems: %d, Trial: %d",
                            noOfDataSystems[n], t
                    ));

                    System.out.println("Loading program...");
                    session.run("call graphEngine.loadParallel(" + noOfDataSystems[n] + ");");

//                    Manager manager = new Manager(10000, db);

                    System.out.println(session.run("match (n) return n limit 100").toString());

                    System.out.println("Executing...");
                    exeTimer.reset();
                    exeTimer.start();
                    session.run("call graphEngine.runParallel(10000);");
                    exeTimer.stop();

                    writer.write(noOfDataSystems[n] + "," + t + "," + exeTimer.getTime());
                    writer.newLine();

                    writer.flush();

                    session.run("match (n) detach delete n;");
                }

                writer.newLine();
                writer.flush();
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testThreadLoader() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.sc2Knapsack;");
        }
    }

    @Test
    public void executeParallel() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            session.run("CALL graphEngine.loadParallel(1000)");

            session.run("CALL graphEngine.runParallel(10000)");
        }
    }
}