package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import parallel.LoadGraphTask;
import parallel.Manager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static graphEngine.TestGraphQueries.viewGraph;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.sc2Knapsack", mode = Mode.SCHEMA)
    public void sc2Knapsack() {
        try {
            int[] noOfDataSystems = {50, 100, 200, 400, 1000, 10000, 20000, 50000};

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
                    // create program
                    LoadGraphTask gLoader = new LoadGraphTask(db, noOfDataSystems[n]);
                    Thread graphThread = new Thread(gLoader);
                    graphThread.start();
                    graphThread.join();

                    Manager manager = new Manager(10000, db);

                    System.out.println("Executing...");
                    exeTimer.reset();
                    exeTimer.start();
                    manager.go();
                    exeTimer.stop();

                    writer.write(noOfDataSystems[n] + "," + t + "," + exeTimer.getTime());
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
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "graphEngine.execute", mode = SCHEMA)
    public void execute(@Name("Max no. of interactions") long maxInteractions) {

        Computer SC = new Computer(db);
        SC.compute((int) maxInteractions);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.preProcess", mode = SCHEMA)
    public void preProcess() {

        Computer SC = new Computer(db);
        SC.preProcess();

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadMany", mode = SCHEMA)
    public void loadMany(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgram);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadManyQuery", mode = SCHEMA)
    public void loadManyQuery(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadParallel", mode = SCHEMA)
    public void loadParallel(@Name("No of data systems") long dataSystems) {

        // create program
        try (Transaction tx = db.beginTx()) {

            TestGraphQueries.knapsack(db, (int) dataSystems);
            tx.success();
        }

    }

    @Procedure(value = "graphEngine.runParallel", mode = SCHEMA)
    public void runParallel(@Name("Max no. of interactions") long maxInteractions) {


        Manager manager = new Manager((int) maxInteractions, db);
        manager.go();
    }

}
