package probability;

import common.Components;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.sc3Knapsack", mode = SCHEMA)
    public void sc3Knapsack() {

        try (Transaction tx = db.beginTx()) {
            int[] nDataSystems = {50, 100, 200, 400, 800, 1000, 2000, 4000, 8000, 16000};

            File file = new File("sc3-selection-execution.csv");
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            writer.append("SC3 Probability\n");
            writer.append("nDataSystems,trial,execution time\n");

            StopWatch timer = new StopWatch();

            for (int n = 0; n < nDataSystems.length; n++) {
                for (int trial = 0; trial < 10; trial++) {

                    System.out.println(String.format("%d solution systems, trial: %d", nDataSystems[n], trial));
                    // load graph
                    db.execute("CALL graphEngine.loadProbability(" + nDataSystems[n] + ", " + nDataSystems[n] + ");");

                    // execute
                    timer.reset();
                    timer.start();
                    db.execute("CALL graphEngine.runProbability(10000);");
                    timer.stop();

                    // log
                    writer.append(String.format("%d,%d,%d\n", nDataSystems[n], trial, timer.getTime()));
                    writer.flush();

                    // delete database
                    db.execute("match (n) detach delete n;");
                }
            }

            tx.success();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Procedure(value = "graphEngine.loadProbability", mode = SCHEMA)
    public void loadProbability(@Name("No of data systems") long dataSystems,
                                @Name("Probability of fittest selection") long nFittest) {

        // create program
        try (Transaction tx = db.beginTx()) {

            // scopes
            Node main = db.createNode(Components.SCOPE);

            // data nodes
            for (int i = 0; i < (int) dataSystems; i++) {
                Node data = db.createNode(Label.label("Data"));
                data.setProperty(Components.distributionA, new int[]{1,0,0,0,0});
                data.setProperty(Components.distributionB, new int[]{1,0,0,0,0});
                main.createRelationshipTo(data, Components.CONTAINS);
            }

            // fittest solution
            Node fittest = db.createNode(Label.label("Data"), Components.FITTEST);
            fittest.setProperty(Components.selection, (int) nFittest);
            fittest.setProperty(Components.distributionA, new int[]{0,0,0,0,1});
            main.createRelationshipTo(fittest, Components.CONTAINS);


            tx.success();
        }

    }

    @Procedure(value = "graphEngine.runProbability", mode = SCHEMA)
    public void runProbability(@Name("Max no. of interactions") long maxInteractions) {

        Manager manager = new Manager((int) maxInteractions, db);
        manager.go();
    }

}
