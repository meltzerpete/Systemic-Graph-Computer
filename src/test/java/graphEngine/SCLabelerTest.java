package graphEngine;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.*;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.List;
import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 12/07/17.
 */
public class SCLabelerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void labellingFitsShouldFindMatches() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
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
                    "(b {name:'B'})," +
                    "(c {name:'C'})," +
                    "(d {name:'D'})," +
                    "(e {name:'E'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(t2)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(sc)-[:CONTAINS]->(d)," +
                    "(sc)-[:CONTAINS]->(e)," +
                    "(t1)-[:CONTAINS]->(b)," +
                    "(t1)-[:CONTAINS]->(c)," +
                    "(t1)-[:CONTAINS]->(d)," +
                    "(d)-[:CONTAINS]->(e)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            SCLabeler labeler = comp.getLabeler();
//            labeler.debug = true;

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            // check for correct FITS1 - should only be from 'co' to 't1'
            Assert.assertTrue(db.getNodeById(1L).getSingleRelationship(Components.FITS1, Direction.OUTGOING).getEndNode().equals(db.getNodeById(2L)));

            // check for correct FITS2
            Stream<Node> targets = db.getAllNodes().stream().filter(node -> node.getId() > 1L);
            Stream<Relationship> FITS2rels = db.getAllRelationships().stream().filter(rel -> rel.isType(Components.FITS2));
            Assert.assertTrue(FITS2rels.allMatch(rel -> rel.getStartNode().getId() == 1L && rel.getEndNode().getId() > 1L));

            tx.success();
            tx.close();
        }
    }

    @Test
    public void labellingFitsShouldNotFindMatches() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            Transaction tx = db.beginTx();

            db.execute("CREATE" +
                    "(sc:SCOPE {key:'sc', name:'sc'})," +
                    "(co:CONTEXT {s1Query:" +
                    "'(n)-[]->(b), (n)-[]->(d), (b)-[]->(c), (d)-[]->(e)'," +
                    "s2Query:'(n)-[]->(m), (n)<-[]-(o), (n)<-[]-(p), (n)<-[]-(q)'," +
                    "name:'co'})," +
                    "(t1:Data {name:'t1'})," +
                    "(t2:Data {name:'t2'})," +
                    "(b:CONTEXT {name:'B', s1Query:'(n)-[]->({name:G})'})," +
                    "(c {name:'C'})," +
                    "(d {name:'D'})," +
                    "(e {name:'E'})," +
                    "(f {name:'F'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(t2)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(sc)-[:CONTAINS]->(d)," +
                    "(sc)-[:CONTAINS]->(e)," +
                    "(sc)-[:CONTAINS]->(f)," +
                    "(t1)-[:CONTAINS]->(b)," +
                    "(t1)-[:CONTAINS]->(c)," +
                    "(t1)-[:CONTAINS]->(d)," +
                    "(d)-[:CONTAINS]->(e)," +
                    "(d)-[:CONTAINS]->(f)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            SCLabeler labeler = comp.getLabeler();
//            labeler.debug = true;

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Assert.assertFalse(db.getAllRelationships().stream().filter(relationship -> relationship.isType(Components.FITS1)).count() > 0);
            Assert.assertFalse(db.getAllRelationships().stream().filter(relationship -> relationship.isType(Components.FITS2)).count() > 0);

            tx.success();
            tx.close();
        }
    }

    @Test
    public void shouldNotMatchGraph_falseDiamond() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            Transaction tx = db.beginTx();

            db.execute("CREATE" +
                    "(sc:SCOPE {key:'sc', name:'sc'})," +
                    "(co:CONTEXT {s1Query:" +
                    "'(n)-[]->(a:FIRST), (n)-[]->(b:FIRST), (a)-[]->(c:SECOND), (b)-[]->(c)'," +
                    "name:'co'})," +
                    "(t1:Data {name:'t1'})," +
                    "(a:FIRST {name:'A'})," +
                    "(b:FIRST {name:'B'})," +
                    "(c:SECOND {name:'C'})," +
                    "(d:SECOND {name:'D'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(a)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(sc)-[:CONTAINS]->(d)," +
                    "(t1)-[:CONTAINS]->(a)," +
                    "(t1)-[:CONTAINS]->(b)," +
                    "(a)-[:CONTAINS]->(c)," +
                    "(b)-[:CONTAINS]->(d)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            SCLabeler labeler = comp.getLabeler();
//            labeler.debug = true;

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Assert.assertFalse(db.getAllRelationships().stream().filter(relationship -> relationship.isType(Components.FITS1)).count() > 0);

            tx.success();
            tx.close();
        }
    }

    @Test
    public void shouldMatchGraph_diamond() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            Transaction tx = db.beginTx();

            db.execute("CREATE" +
                    "(sc:SCOPE {key:'sc', name:'sc'})," +
                    "(co:CONTEXT {s1Query:" +
                    "'(n)-[]->(a:FIRST), (n)-[]->(b:FIRST), (a)-[]->(c:SECOND), (b)-[]->(c)'," +
                    "name:'co'})," +
                    "(t1:Data {name:'t1'})," +
                    "(a:FIRST {name:'A'})," +
                    "(b:FIRST {name:'B'})," +
                    "(c:SECOND {name:'C'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(a)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(t1)-[:CONTAINS]->(a)," +
                    "(t1)-[:CONTAINS]->(b)," +
                    "(a)-[:CONTAINS]->(c)," +
                    "(b)-[:CONTAINS]->(c)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            SCLabeler labeler = comp.getLabeler();
//            labeler.debug = true;

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Assert.assertTrue(db.getAllRelationships().stream().filter(relationship -> relationship.isType(Components.FITS1)).count() == 1);

            tx.success();
            tx.close();
        }
    }

    @Test
    public void shouldMatchGraph_extendedDiamond() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            Transaction tx = db.beginTx();

            db.execute("CREATE" +
                    "(sc:SCOPE {key:'sc', name:'sc'})," +
                    "(co:CONTEXT {s1Query:" +
                    "'(n)-[]->(a:FIRST), (a)-[]->(c:SECOND), (n)-[]->(c)'," +
                    "name:'co'})," +
                    "(t1:Data {name:'t1'})," +
                    "(d:FIRST {name:'D'})," +
                    "(e:SECOND {name:'E'})," +
                    "(a:FIRST {name:'A'})," +
                    "(b:FIRST {name:'B'})," +
                    "(c:SECOND {name:'C'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(a)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(sc)-[:CONTAINS]->(d)," +
                    "(sc)-[:CONTAINS]->(e)," +
                    "(t1)-[:CONTAINS]->(a)," +
                    "(t1)-[:CONTAINS]->(c)," +
                    "(t1)-[:CONTAINS]->(d)," +
                    "(d)-[:CONTAINS]->(e)," +
                    "(a)-[:CONTAINS]->(c)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            SCLabeler labeler = comp.getLabeler();
//            labeler.debug = true;

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Assert.assertTrue(db.getAllRelationships().stream().filter(relationship -> relationship.isType(Components.FITS1)).count() == 1);

            tx.success();
            tx.close();
        }
    }

    @Test
    public void shouldMatchGraph_allTriangles() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
             Session session = driver.session()
        ) {
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            Transaction tx = db.beginTx();

            db.execute("CREATE" +
                    "(sc:SCOPE {key:'sc', name:'sc'})," +
                    "(co:CONTEXT {s1Query:" +
                    "'(n)-[]->(a), (a)-[]->(c), (n)-[]->(c)'," +
                    "name:'co'})," +
                    "(t1:Data {name:'t1'})," +
                    "(a {name:'A'})," +
                    "(b {name:'B'})," +
                    "(c {name:'C'})," +
                    "(d {name:'D'})," +
                    "(e {name:'E'})," +
                    "(sc)-[:CONTAINS]->(co)," +
                    "(sc)-[:CONTAINS]->(t1)," +
                    "(sc)-[:CONTAINS]->(a)," +
                    "(sc)-[:CONTAINS]->(b)," +
                    "(sc)-[:CONTAINS]->(c)," +
                    "(sc)-[:CONTAINS]->(d)," +
                    "(sc)-[:CONTAINS]->(e)," +
                    "(b)-[:CONTAINS]->(d)," +
                    "(b)-[:CONTAINS]->(e)," +
                    "(t1)-[:CONTAINS]->(a)," +
                    "(t1)-[:CONTAINS]->(c)," +
                    "(t1)-[:CONTAINS]->(d)," +
                    "(t1)-[:CONTAINS]->(e)," +
                    "(a)-[:CONTAINS]->(b)," +
                    "(a)-[:CONTAINS]->(c)," +
                    "(b)-[:CONTAINS]->(c)," +
                    "(d)-[:CONTAINS]->(e)");

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Computer comp = new Computer(db);

            SCLabeler labeler = comp.getLabeler();
//            labeler.debug = true;

            comp.preProcess();

            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            Assert.assertTrue(db.getAllRelationships().stream().filter(relationship -> relationship.isType(Components.FITS1)).count() == 3);

            tx.success();
            tx.close();
        }
    }

//    @Test
//    public void recursiveTest() {
//        recursiveTest(new LinkedList<>());
//    }

    private static void recursiveTest(List<String> list) {
        if (list.size() == 3)
            return;
        System.out.println("before: " + list);
        list.add("a");
        recursiveTest(list);
        System.out.println("after: " + list);
    }
}