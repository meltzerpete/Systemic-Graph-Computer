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
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Created by Pete Meltzer on 14/08/17.
 */
public class ExecuteTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(Execute.class);


    @Test
    public void executeProbability() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            int[] nDataSystems = {50, 100, 200, 400, 800, 1000, 2000, 4000, 8000, 16000};

            File file = new File("sc3-probability-execution.csv");
            BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));

            writer.append("SC3 Probability\n");
            writer.append("nDataSystems,trial,execution time\n");

            StopWatch timer = new StopWatch();

            for (int n = 0; n < nDataSystems.length; n++) {
                for (int trial = 0; trial < 10; trial++) {

                    System.out.println(String.format("%d solution systems, trial: %d", nDataSystems[n], trial));
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

    static int W = 80;
    static int[] w = {15,20,1,3,8,2,16,17,11,19,10,5,18,4,7,9};
    static int[] v = {20,14,12,9,5,17,7,6,1,11,4,19,13,2,15,3};

    private static int fitness(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += v[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    private static int weight(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += w[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    private static char guard(char x) {

        while (weight(x) > W) {

            char bitMask = (char) (0xffff ^ ((char) (0x0001 << ThreadLocalRandom.current().nextInt( 16))));
            x &= bitMask;

        }
        return x;
    }

    @Test
    public void solutions() throws Throwable {

        for (int t = 0; t < 10; t++) {
            StopWatch timer = new StopWatch();
            timer.start();
            int possible = 0;
            char solution = 0x0000;
            char best = 0x0000;

            for (int i = 0; i < (int) 0xffff; i++) {
                if (guard(solution) == solution) {
                    possible++;
                    if (fitness(solution) > best) best = solution;
                }
                solution++;
            }
            timer.stop();

            System.out.println(String.format("%,d x 10e-3 s", timer.getTime()));
            System.out.println(possible + " out of " + (int) Math.pow(2, 16));
            System.out.println(fitness(best) + " - " + Integer.toBinaryString(best));
        }
    }
}