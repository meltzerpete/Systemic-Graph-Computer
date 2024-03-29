package parallel;

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

import static org.neo4j.procedure.Mode.SCHEMA;

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
                    // clear database and create program
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
