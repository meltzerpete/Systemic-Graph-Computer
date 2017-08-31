package probability;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

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

            int[] nDataSystems = {1000, 2000, 4000, 8000, 16000};

            File file = new File("sc3-probability-execution.csv");
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            writer.append("SC3 Probability\n");
            writer.append("nDataSystems,trial,execution time\n");

            StopWatch timer = new StopWatch();

            for (int n = 0; n < nDataSystems.length; n++) {
                for (int trial = 0; trial < 10; trial++) {

                    // load graph
                    session.run("CALL graphEngine.loadProbability(" + nDataSystems[n] + ", " + nDataSystems[n] + ");");

                    // execute
                    timer.reset();
                    timer.start();
                    session.run("CALL graphEngine.runProbability(10000);");
                    timer.stop();

                    // log
                    writer.append(String.format("%d,%d,%d\n", nDataSystems[n], trial, timer.getTime()));
                    writer.flush();

                    // delete database
                    session.run("match (n) detach delete n;");
                }
            }

            writer.close();
        }
    }

}