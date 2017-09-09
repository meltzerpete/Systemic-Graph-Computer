package graphEngine;

import common.TestGraphQueries;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by Pete Meltzer on 11/07/17.
 * <p>All experiment procedures and some additional procedures
 * for debugging. Each may be called from the cypher-shell or
 * browser interface.</p>
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.sc1ExampleProgram", mode = Mode.SCHEMA)
    public void sc1ExampleProgram() {

        try {

            File file = new File("sc1-example-program.csv");
            FileWriter fwriter = new FileWriter(file,true);
            BufferedWriter writer = new BufferedWriter(fwriter);

            StopWatch timer = new StopWatch();
            writer.write("trial,time (ms)\n");

            for (int i = 0; i < 100; i++) {

                System.out.println("Trial " + i + ":");

                db.execute(TestGraphQueries.programWithQueryMatching);

                timer.reset();
                timer.start();
                Computer sc = new Computer(db);
                sc.preProcess();
                sc.compute(10);
                timer.stop();
                writer.write(String.format("%d,%d\n", i, timer.getTime()));
                writer.flush();

                db.execute("match (n) detach delete n");
            }
            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "graphEngine.sc1TerminatingMany", mode = Mode.SCHEMA)
    public void sc1TerminatingMany() {

        for (int i = 0; i < 1000; i++) {
            System.out.println("Loading graph " + i + "...");
            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);
        }

        Computer sc = new Computer(db);

        System.out.println("preProcessing...");
        sc.preProcess();

        File file = new File("sc1-terminating-many.csv");
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));

            writer.write("interactions,not started,mid,finished\n");
            writer.write("0,1000,0,0");

            for (int i = 0; i < 20; i++) {
                sc.compute(100);

                int notStarted = (int) db.execute(
                        "match (n:CONTEXT)-[r]->()\n" +
                        "with n, count(r) as rel_count\n" +
                        "where rel_count=4\n" +
                        "return n;").columnAs("n").stream().count();
                int mid = (int) db.execute(
                        "match (n:CONTEXT)-[r]->()\n" +
                                "with n, count(r) as rel_count\n" +
                                "where rel_count=3\n" +
                                "return n;").columnAs("n").stream().count();
                int finished = (int) db.execute(
                        "match (n:CONTEXT)-[r]->()\n" +
                                "with n, count(r) as rel_count\n" +
                                "where rel_count=2\n" +
                                "return n;").columnAs("n").stream().count();

                writer.write(String.format("%d,%d,%d,%d\n", (i+1)*100, notStarted, mid, finished));
                writer.flush();
            }

            writer.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "graphEngine.sc1Knapsack", mode = Mode.SCHEMA)
    public void sc1Knapsack() {
        try {
            int[] noOfDataSystems = {50, 100, 200, 400, 800, 1000};

            File file = new File("sc1-knapsack.csv");
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

                        TestGraphQueries.knapsack(db, noOfDataSystems[n]);
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

                    db.execute("match (n) detach delete n;");
                }

                writer.newLine();
                writer.flush();
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "graphEngine.sc1KnapsackFine", mode = Mode.SCHEMA)
    public void knapsackFine() {
        int[] noOfDataSystems = {50, 100, 200, 400, 800, 1000};

        File file = new File("sc1-knapsack-fine.csv");
        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(file,true);
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

                        TestGraphQueries.knapsack(db, noOfDataSystems[n]);
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

                db.execute("match (n) detach delete n;");
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "graphEngine.execute", mode = Mode.SCHEMA)
    public void execute(@Name("Max no. of interactions") long maxInteractions) {

        Computer SC = new Computer(db);
        SC.compute((int) maxInteractions);
    }

    @Procedure(value = "graphEngine.preProcess", mode = Mode.SCHEMA)
    public void preProcess() {

        Computer SC = new Computer(db);
        SC.preProcess();
    }

    @Procedure(value = "graphEngine.loadOriginal", mode = Mode.SCHEMA)
    public void loadOriginal(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.programWithQueryMatching);

    }

    @Procedure(value = "graphEngine.loadMany", mode = Mode.SCHEMA)
    public void loadMany(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.programWithQueryMatching);
    }

    @Procedure(value = "graphEngine.loadManyTerminating", mode = Mode.SCHEMA)
    public void loadManyTerminating(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);
    }

    @Procedure(value = "graphEngine.loadKnapsack", mode = Mode.SCHEMA)
    public void loadKnapsack(@Name("No. of data systems") long n) {

        TestGraphQueries.knapsack(db, (int) n);
    }


}
