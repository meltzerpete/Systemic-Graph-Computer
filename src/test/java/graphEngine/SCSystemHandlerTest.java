package graphEngine;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.HashSet;
import java.util.stream.Stream;

import static org.junit.Assert.assertTrue;
/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class SCSystemHandlerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void getAllSystemsInScope() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();
            db.execute(TestGraphQueries.systemsWithShapeProperties);
            SCSystemHandler handler = new Computer(db).getHandler();

            // cheack all expected nodes for each scope are contained in the query results
            db.findNodes(Components.SCOPE).forEachRemaining(node -> {

                Stream<Node> methodRes = handler.getAllSystemsInScope(node);

                Result testRes = db.execute("match (n:SCOPE {name:'"
                        + node.getProperty("name")
                        + "'})-[:CONTAINS]->(m) return m.name");

                methodRes.forEach(node1 -> {
                    assertTrue(testRes.resultAsString().contains((CharSequence) node1.getProperty("name")));
                });

            });

            // cheack the query returns the same no. of results or each scope
            db.findNodes(Components.SCOPE).forEachRemaining(node -> {

                int methodCount = (int) handler.getAllSystemsInScope(node).count();

                int testCount = (int) db.execute("match (n:SCOPE {name:'"
                        + node.getProperty("name")
                        + "'})-[:CONTAINS]->(m) return m.name").stream().count();

                assertTrue(methodCount == testCount);

            });

            tx.success();
            tx.close();
        }
    }

    @Test
    public void getContextsInScope() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();
            db.execute(TestGraphQueries.systemsWithShapeProperties);
            SCSystemHandler handler = new Computer(db).getHandler();

            // cheack all expected nodes for each scope are contained in the query results
            db.findNodes(Components.SCOPE).forEachRemaining(node -> {

                Stream<Node> methodRes = handler.getContextsInScope(node);

                Result testRes = db.execute("match (n:SCOPE {name:'"
                        + node.getProperty("name")
                        + "'})-[:CONTAINS]->(m:CONTEXT) return m.name");

                methodRes.forEach(node1 -> {
                    assertTrue(testRes.resultAsString().contains((CharSequence) node1.getProperty("name")));
                });

            });

            // check the query returns the same no. of results or each scope
            db.findNodes(Components.SCOPE).forEachRemaining(node -> {

                int methodCount = (int) handler.getContextsInScope(node).count();

                int testCount = (int) db.execute("match (n:SCOPE {name:'"
                        + node.getProperty("name")
                        + "'})-[:CONTAINS]->(m:CONTEXT) return m.name").stream().count();

                assertTrue(methodCount == testCount);

            });

            tx.success();
            tx.close();
        }
    }

    @Test
    public void getParentScopes() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();
            db.execute(TestGraphQueries.systemsWithShapeProperties);
            SCSystemHandler handler = new Computer(db).getHandler();

            // check all expected scopes for each node are contained in the query results
            db.getAllNodes().stream().forEach(node -> {

                if (node.hasRelationship(Components.CONTAINS, Direction.INCOMING)) {
                    Stream<Node> methodRes = handler.getParentScopes(node);

                    Result testRes = db.execute("match (n {name:'"
                            + node.getProperty("name")
                            + "'})<-[:CONTAINS]-(m:SCOPE) return m.name");

                    methodRes.forEach(node1 -> {
                        assertTrue(
                                testRes.resultAsString().contains(
                                        (CharSequence) node1.getProperty("name")));
                    });
                }

            });

            // check the query returns the same no. of results for each node
            db.findNodes(Components.SCOPE).forEachRemaining(node -> {

                int methodCount = (int) handler.getParentScopes(node).count();

                int testCount = (int) db.execute("match (n {name:'"
                        + node.getProperty("name")
                        + "'})<-[:CONTAINS]-(m:SCOPE) return m.name").stream().count();

                assertTrue(methodCount == testCount);

            });

            tx.success();
            tx.close();
        }
    }

    @Test
    public void getRandomReady() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();

            for (int i = 0; i < 500; i++)
                db.createNode(Math.random() > 0.5 ? Components.READY : Components.SCOPE);

            SCSystemHandler handler = new Computer(db).getHandler();

            HashSet<Node> resultSet = new HashSet<>();

            for (int i = 0; i < 10 * db.findNodes(Components.READY).stream().count(); i++) {
                Node randReady = handler.getRandomReady();
                assertTrue(randReady.hasLabel(Components.READY));
                resultSet.add(randReady);
            }

            assertTrue(resultSet.size() > db.findNodes(Components.READY).stream().count() * 0.9);

            tx.success();
            tx.close();
        }
    }

    @Test
    public void getRandomS1() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()) {

            GraphDatabaseService db = neo4j.getGraphDatabaseService();
            Transaction tx = db.beginTx();
            SCSystemHandler handler = new Computer(db).getHandler();

            Node context = db.createNode(Components.CONTEXT);

            for (int i = 0; i < 500; i++) {
                db.createNode(Label.label("IN")).createRelationshipTo(context, Components.FITS1);
                context.createRelationshipTo(db.createNode(), Components.FITS1);
            }

            HashSet<Node> resultSet = new HashSet<>();

            for (int i = 0; i < 5000; i++) {
                Node randS1 = handler.getRandomS1(context);
                assertTrue(randS1.hasRelationship(Components.FITS1, Direction.INCOMING));
                resultSet.add(randS1);
            }

            assertTrue(resultSet.size() > 250 * 0.9);

            tx.success();
            tx.close();
        }
    }
}