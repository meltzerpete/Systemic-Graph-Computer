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
 * Created by Pete Meltzer on 12/07/17.
 */
public class SCLabelerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void labellingFits() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session();
             ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            Transaction tx = db.beginTx();

            db.execute("CREATE" +
                    "(sc:SCOPE {key:'sc', name:'sc'})," +
                    "(co:CONTEXT {s1Query:" +
                    "'(n)-[]->(b), (n)-[]->(d), (b)-[]->(c)'," +
                    "s2Query:'(n)'," +
                    "name:'co'})," +
                    "(t1:Data {name:'t1'})," +
                    "(t2:Data {name:'t2'})," +
                    "(b {name:'b'})," +
                    "(c {name:'c'})," +
                    "(d {name:'d'})," +
                    "(e {name:'e'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(t2)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(sc)-[:CONTAINS]->(d)," +
                    "(sc)-[:CONTAINS]->(e)," +
                    "(t1)-[:CONTAINS]->(b)," +
                    "(t1)-[:CONTAINS]->(d)," +
                    "(t1)-[:CONTAINS]->(c)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            tx.success();
            tx.close();
        }
    }

    @Test
    public void labellingReady() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session();
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();

            Computer comp = new Computer(db);


        }
    }
}