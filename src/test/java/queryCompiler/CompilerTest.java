package queryCompiler;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by Pete Meltzer on 24/07/17.
 */
public class CompilerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void testCompiler() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            Compiler compiler = new Compiler();
            StopWatch timer = new StopWatch();
            timer.start();
//            compiler.compile("(n:Data1:Data {function:NOP, new:pete})-[:FITS_2]->(m)," +
//                    "(n)<-[:FITS_2]-(o), (m)-[]-(o), (m)-[]->(p), (n)-[]-(p), " +
//                    "(p)-[:FIT]->(o)");
            compiler.compile("(n)-[]->(m), (m)-[:FITS_1]->(n)");
            timer.stop();
            System.out.println(String.format("Compilation took: %,d x 10e-3 s", timer.getTime()));
        }

    }

//    @Test
//    public void testDepth() throws Throwable {
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
//                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
//             Session session = driver.session()) {
//
//            Compiler comp = new Compiler();
//
//            Vertex v = comp.compile("(n)-[]->(b)," +
//                    "(n)-[]->(c)," +
//                    "(b)-[]->(d)," +
//                    "(b)-[]->(e)," +
//                    "(b)-[]->(f)," +
//                    "(f)-[]->(c)");
//
//            System.out.println(comp.getDepth(v));
//
//        }
//
//    }



}