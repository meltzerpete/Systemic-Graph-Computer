package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by Pete Meltzer on 12/07/17.
 */
public class MoreQueryTesting {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

//    @Test
//    public void testBeerWithCypher() throws Throwable {
//        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
//                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
//             Session session = driver.session()
//        ) {
//            GraphDatabaseService db = neo4j.getGraphDatabaseService();
//
//            StopWatch timer = new StopWatch();
//
//            for (int i = 0; i < 10; i++) {
//                Transaction tx = db.beginTx();
//
//                FileReader r = new FileReader(new File("./beers-query.txt"));
//                BufferedReader reader = new BufferedReader(r);
//
//                StringBuilder string = new StringBuilder();
//
//                reader.lines().forEach(s -> string.append(s));
//
//                System.out.println("Populating database...");
//                System.out.println(db.execute(string.toString()).resultAsString());
//
//                System.out.println("Executing query...");
//                timer.reset();
//                timer.start();
//                System.out.println(db.execute("match" +
//                        "(n:Brewery)<-[:BREWED_AT]-(a:Beer)," +
//                        "(n)<-[:BREWED_AT]-(b:Beer)," +
//                        "(a)-[:BEER_STYLE]->(s:Style)," +
//                        "(b)-[:BEER_STYLE]->(s)" +
//                        "return count(distinct n)" +
//                        "").resultAsString());
//                timer.stop();
//
//                System.out.println(String.format("Query time: %,d x 10e-9 s", timer.getNanoTime()));
//
//                System.out.println("Deleting database...");
//                db.execute("match (n) detach delete n");
//
//                tx.success();
//                tx.close();
//            }
//
//        }
//    }

    @Test
    public void testBeerWithSC() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            StopWatch timer = new StopWatch();

            for (int i = 0; i < 10; i++) {
                Transaction tx = db.beginTx();

                FileReader r = new FileReader(new File("./beers-query.txt"));
                BufferedReader reader = new BufferedReader(r);

                StringBuilder string = new StringBuilder();

                reader.lines().forEach(s -> string.append(s));

                System.out.println("Populating database...");
                System.out.println(db.execute(string.toString()).resultAsString());

                System.out.println("Setting scope and context...");
                Node scope = db.createNode(Components.SCOPE);
                scope.setProperty("name","scope");
                Node context = db.createNode(Components.CONTEXT);
                context.setProperty("name","context");
                context.setProperty("s1Query",
                        "(n:Brewery)<-[:BREWED_AT]-(a:Beer)," +
                        "(n)<-[:BREWED_AT]-(b:Beer)," +
                        "(a)-[:BEER_STYLE]->(s:Style)," +
                        "(b)-[:BEER_STYLE]->(s)" +
                                "");
                scope.createRelationshipTo(context, Components.CONTAINS);
                db.getAllNodes().forEach(node -> scope.createRelationshipTo(node, Components.CONTAINS));

                tx.success();
                tx.close();

                Computer comp = new Computer(db);
                SCLabeler labeler = comp.getLabeler();

                comp.withCypher = false;
                labeler.debug = false;

                comp.preProcess();
                comp.compute(10);


//                System.out.println("Executing search...");
//                timer.reset();
//                timer.start();
//                labeler.createFitsInScope(scope);
//                timer.stop();

                tx = db.beginTx();
                long count = db.getAllRelationships().stream()
                        .filter(relationship -> relationship.isType(Components.FITS1))
                        .count();

                System.out.println("count: " + count);

//                        .forEach(relationship -> System.out.println(relationship.getEndNode().getProperty("name")));

//                System.out.println(String.format("Search time: %,d x 10e-9 s", timer.getNanoTime()));

                System.out.println("Deleting database...");
                db.execute("match (n) detach delete n");

                tx.success();
                tx.close();
            }

        }
    }

    @Test
    public void testBeerWithCypher() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            StopWatch timer = new StopWatch();

            for (int i = 0; i < 10; i++) {
                Transaction tx = db.beginTx();

                FileReader r = new FileReader(new File("./beers-query.txt"));
                BufferedReader reader = new BufferedReader(r);

                StringBuilder string = new StringBuilder();

                reader.lines().forEach(s -> string.append(s));

                System.out.println("Populating database...");
                System.out.println(db.execute(string.toString()).resultAsString());

                System.out.println("Setting scope and context...");
                Node scope = db.createNode(Components.SCOPE);
                scope.setProperty("name","scope");
                Node context = db.createNode(Components.CONTEXT);
                context.setProperty("name","context");
                context.setProperty("s1Query",
                        "(n:Brewery)<-[:BREWED_AT]-(a:Beer)," +
                                "(n)<-[:BREWED_AT]-(b:Beer)," +
                                "(a)-[:BEER_STYLE]->(s:Style)," +
                                "(b)-[:BEER_STYLE]->(s)" +
                                "");
                scope.createRelationshipTo(context, Components.CONTAINS);
                db.getAllNodes().forEach(node -> scope.createRelationshipTo(node, Components.CONTAINS));

                tx.success();
                tx.close();

                Computer comp = new Computer(db);
                SCLabeler labeler = comp.getLabeler();

                comp.withCypher = true;
                labeler.debug = false;

                comp.preProcess();
                comp.compute(10);


//                System.out.println("Executing search...");
//                timer.reset();
//                timer.start();
//                labeler.createFitsInScope(scope);
//                timer.stop();

                tx = db.beginTx();
                long count = db.getAllRelationships().stream()
                        .filter(relationship -> relationship.isType(Components.FITS1))
                        .count();

                System.out.println("count: " + count);

//                        .forEach(relationship -> System.out.println(relationship.getEndNode().getProperty("name")));

//                System.out.println(String.format("Search time: %,d x 10e-9 s", timer.getNanoTime()));

                System.out.println("Deleting database...");
                db.execute("match (n) detach delete n");

                tx.success();
                tx.close();
            }

        }
    }
}