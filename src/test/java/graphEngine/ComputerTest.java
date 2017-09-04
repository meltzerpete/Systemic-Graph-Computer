package graphEngine;

import common.Components;
import common.TestGraphQueries;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

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

                System.out.println("Populating database...");
                for (int j = 0; j < 1000; j++)
                    db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

                Computer comp = new Computer(db);

                System.out.println("Pre-processing...");
                comp.preProcess();

//            String state = db.execute(TestGraphQueries.viewGraph).resultAsString();
//            System.out.println(state);

                System.out.println("Executing...");
                comp.compute(100000);

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

    @Test
    public void geneticKnapsack() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            int[] noOfDataSystems = {50, 100, 200, 400, 800, 1000};

            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            File file = new File("geneticKnapsackTiming.csv");
            FileWriter fwriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fwriter);

            writer.write("Knapsack Testing\n");
            writer.write("No. of Solution Systems, Trial, Pre-processing, Execution\n");

            StopWatch preTimer = new StopWatch();
            StopWatch exeTimer = new StopWatch();

            for (int n = 0; n < 5; n++) {
                for (int t = 0; t < 5; t++) {

                    System.out.println(String.format(
                            "No. of solution systems: %d, Trial: %d",
                            noOfDataSystems[n], t
                    ));

                    System.out.println("Loading program...");
                    // create program
                    try (Transaction tx = db.beginTx()) {

                        // scopes
                        Node main = db.createNode(Components.SCOPE);
                        Node computation = db.createNode(Components.SCOPE, Components.COMPUTATION);

                        // contexts
                        Node initializer = db.createNode(Components.CONTEXT);
                        initializer.setProperty(Components.function, "INITIALIZE");
                        initializer.setProperty(Components.s1Query, "(:UNINITIALIZED)");
                        initializer.setProperty(Components.s2Query, "(:COMPUTATION)");

                        Node binMutate = db.createNode(Components.CONTEXT);
                        binMutate.setProperty(Components.function, "BINARYMUTATE");
                        binMutate.setProperty(Components.s1Query, "(:INITIALIZED)");
                        binMutate.setProperty(Components.s2Query, "(:INITIALIZED)");

                        Node onePointCross = db.createNode(Components.CONTEXT);
                        onePointCross.setProperty(Components.function, "ONEPOINTCROSS");
                        onePointCross.setProperty(Components.s1Query, "(:INITIALIZED)");
                        onePointCross.setProperty(Components.s2Query, "(:INITIALIZED)");

                        Node uniformCross = db.createNode(Components.CONTEXT);
                        uniformCross.setProperty(Components.function, "UNIFORMCROSS");
                        uniformCross.setProperty(Components.s1Query, "(:INITIALIZED)");
                        uniformCross.setProperty(Components.s2Query, "(:INITIALIZED)");

                        Node output = db.createNode(Components.CONTEXT);
                        output.setProperty(Components.function, "OUTPUT");
                        output.setProperty(Components.s1Query, "(:FITTEST)");
                        output.setProperty(Components.s2Query, "(:INITIALIZED)");

                        main.createRelationshipTo(output, Components.CONTAINS);
                        main.createRelationshipTo(initializer, Components.CONTAINS);
                        main.createRelationshipTo(computation, Components.CONTAINS);

                        computation.createRelationshipTo(binMutate, Components.CONTAINS);
                        computation.createRelationshipTo(onePointCross, Components.CONTAINS);
                        computation.createRelationshipTo(uniformCross, Components.CONTAINS);

                        // data nodes
                        for (int i = 0; i < noOfDataSystems[n]; i++) {
                            Node data = db.createNode(Components.DATA, Components.UNINITIALIZED);
                            main.createRelationshipTo(data, Components.CONTAINS);
                        }

                        // fittest solution
                        Node fittest = db.createNode(Components.DATA, Components.FITTEST);
                        main.createRelationshipTo(fittest, Components.CONTAINS);


                        tx.success();
                    }

                    Computer comp = new Computer(db);

                    System.out.println("Pre-processing...");
                    preTimer.reset();
                    preTimer.start();
                    comp.preProcess();
                    preTimer.stop();

                    System.out.println("Executing...");
                    exeTimer.reset();
                    exeTimer.start();
                    comp.compute(10000);
                    exeTimer.stop();

                    writer.write(noOfDataSystems[n] + ", " + t + ", " + preTimer.getTime() + ", " + exeTimer.getTime());
                    writer.newLine();

                    writer.flush();
                }

                writer.newLine();
                writer.flush();
            }

            writer.close();
        }
    }

    @Test
    public void geneticKnapsackFine() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            int[] noOfDataSystems = {50, 100, 200, 400, 800, 1000};

            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            File file = new File("geneticKnapsackFineTiming.csv");
            FileWriter fwriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fwriter);

            writer.write("Knapsack Testing\n");
            writer.write("No. of Solution Systems, Trial, Pre-processing, Execution\n");

            StopWatch preTimer = new StopWatch();
            StopWatch exeTimer = new StopWatch();

            for (int n = 0; n < noOfDataSystems.length; n++) {
                for (int t = 0; t < 5; t++) {

                    System.out.println(String.format(
                            "No. of solution systems: %d, Trial: %d",
                            noOfDataSystems[n], t
                    ));

                    System.out.println("Loading program...");
                    // create program
                    try (Transaction tx = db.beginTx()) {

                        // scopes
                        Node main = db.createNode(Components.SCOPE);
                        Node computation = db.createNode(Components.SCOPE, Label.label("COMPUTATION"));

                        // contexts
                        Node initializer = db.createNode(Components.CONTEXT);
                        initializer.setProperty(Components.function, "INITIALIZE");
                        initializer.setProperty(Components.s1Query, "(:UNINITIALIZED)");
                        initializer.setProperty(Components.s2Query, "(:COMPUTATION)");

                        Node binMutate = db.createNode(Components.CONTEXT);
                        binMutate.setProperty(Components.function, "BINARYMUTATE");
                        binMutate.setProperty(Components.s1Query, "(:INITIALIZED)");
                        binMutate.setProperty(Components.s2Query, "(:INITIALIZED)");

                        Node onePointCross = db.createNode(Components.CONTEXT);
                        onePointCross.setProperty(Components.function, "ONEPOINTCROSS");
                        onePointCross.setProperty(Components.s1Query, "(:INITIALIZED)");
                        onePointCross.setProperty(Components.s2Query, "(:INITIALIZED)");

                        Node uniformCross = db.createNode(Components.CONTEXT);
                        uniformCross.setProperty(Components.function, "UNIFORMCROSS");
                        uniformCross.setProperty(Components.s1Query, "(:INITIALIZED)");
                        uniformCross.setProperty(Components.s2Query, "(:INITIALIZED)");

                        Node output = db.createNode(Components.CONTEXT);
                        output.setProperty(Components.function, "OUTPUT");
                        output.setProperty(Components.s1Query, "(:FITTEST)");
                        output.setProperty(Components.s2Query, "(:INITIALIZED)");

                        main.createRelationshipTo(output, Components.CONTAINS);
                        main.createRelationshipTo(initializer, Components.CONTAINS);
                        main.createRelationshipTo(computation, Components.CONTAINS);

                        computation.createRelationshipTo(binMutate, Components.CONTAINS);
                        computation.createRelationshipTo(onePointCross, Components.CONTAINS);
                        computation.createRelationshipTo(uniformCross, Components.CONTAINS);

                        // data nodes
                        for (int i = 0; i < noOfDataSystems[n]; i++) {
                            Node data = db.createNode(Label.label("Data"), Label.label("UNINITIALIZED"));
                            main.createRelationshipTo(data, Components.CONTAINS);
                        }

                        // fittest solution
                        Node fittest = db.createNode(Label.label("Data"), Label.label("FITTEST"));
                        main.createRelationshipTo(fittest, Components.CONTAINS);


                        tx.success();
                    }

                    Computer comp = new Computer(db);

                    System.out.println("Pre-processing...");
                    preTimer.reset();
                    preTimer.start();
                    comp.preProcess();
                    preTimer.stop();

                    writer.write(noOfDataSystems[n] + ", " + t + ", " + preTimer.getTime() + ", " + exeTimer.getTime());
                    writer.newLine();
                    writer.flush();

                    System.out.println("Executing...");
                    writer.write("tx, check, getRandomReady, getFunc, getPair, acquireLocks, perfFunc, rmFITS/READY, newFITS/READY, txRelease\n");
                    exeTimer.reset();
                    exeTimer.start();
                    comp.compute(500, writer);
                    exeTimer.stop();

                }

                writer.newLine();
                writer.flush();
            }

            writer.close();
        }
    }
}