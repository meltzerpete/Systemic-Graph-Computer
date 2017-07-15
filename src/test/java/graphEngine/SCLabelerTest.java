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

import java.util.LinkedList;
import java.util.List;

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

            Computer comp = new Computer(db);
            SCLabeler scLabeler = comp.getLabeler();

            Node dataContext = db.createNode(Components.CONTEXT);
            Node wasteContext = db.createNode(Components.CONTEXT);
            Node emptyContext = db.createNode(Components.CONTEXT);
            Node emptyContext2 = db.createNode(Components.CONTEXT);

            dataContext.setProperty("s1Labels", new String[]{"DATA", "SCOPE"});
            wasteContext.setProperty("s1Labels", new String[]{"WASTE"});
            emptyContext.setProperty("s1Labels", new String[]{});
            // emptyContext2 - don't set any properties for s1Labels

            List<Node> contexts = new LinkedList<>();
            contexts.add(dataContext);
            contexts.add(wasteContext);
            contexts.add(emptyContext);
            contexts.add(emptyContext2);

            List<Node> nodes = new LinkedList<>();

            for (int i = 0; i < 10; i++) {
                nodes.add(db.createNode());
            }

            nodes.forEach(node -> {
                dataContext.createRelationshipTo(node, Components.CONTAINS);
                wasteContext.createRelationshipTo(node, Components.CONTAINS);
                emptyContext.createRelationshipTo(node, Components.CONTAINS);
            });

            nodes.get(3).addLabel(Components.SCOPE);
            nodes.get(3).addLabel(Components.DATA);
            nodes.get(6).addLabel(Components.SCOPE);
            nodes.get(7).addLabel(Components.DATA);
            nodes.get(8).addLabel(Components.WASTE);

            nodes.forEach(node -> System.out.println(node.getLabels()));

            Node scope = db.createNode(Components.SCOPE);
            contexts.forEach(context -> scope.createRelationshipTo(context, Components.CONTAINS));
            nodes.forEach(node -> scope.createRelationshipTo(node, Components.CONTAINS));

//            Method fits1 = SCLabeler.class.getDeclaredMethod("fitsLabels", Node.class, Node.class, String.class);
//            fits1.setAccessible(true);

//            contexts.forEach(context -> {
//                nodes.forEach(node -> {
//                    try {
//                        fits1.invoke(scLabeler, context, node, Components.s1Labels);
//                    } catch (IllegalAccessException e) {
//                        e.printStackTrace();
//                    } catch (InvocationTargetException e) {
//                        e.printStackTrace();
//                    }
//
//                });
//            });

            String result = db.execute(TestGraphQueries.viewGraph).resultAsString();
            System.out.println(result);

            scLabeler.labelFitsInScope(scope);

            result = db.execute(TestGraphQueries.viewGraph).resultAsString();
            System.out.println(result);

            ResourceIterator<Relationship> iterator
                    = (ResourceIterator<Relationship>) dataContext.getRelationships(Components.FITS1, Direction.OUTGOING).iterator();
            Assert.assertTrue(
                    iterator.stream()
                            .map(relationship -> relationship.getEndNode().hasLabel(Components.DATA)
                                                    && relationship.getEndNode().hasLabel(Components.SCOPE))
                            .reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2));

            iterator
                    = (ResourceIterator<Relationship>) dataContext.getRelationships(Components.FITS1, Direction.OUTGOING).iterator();
            Assert.assertTrue(iterator.stream().count() == 1);

            iterator
                    = (ResourceIterator<Relationship>) wasteContext.getRelationships(Components.FITS1, Direction.OUTGOING).iterator();
            Assert.assertTrue(
                    iterator.stream()
                            .map(relationship -> relationship.getEndNode().hasLabel(Components.WASTE))
                            .reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2));

            iterator
                    = (ResourceIterator<Relationship>) wasteContext.getRelationships(Components.FITS1, Direction.OUTGOING).iterator();
            Assert.assertTrue(iterator.stream().count() == 1);

            Assert.assertTrue(
                    db.getAllNodes().stream()
                            .filter(node -> !node.hasLabel(Components.CONTEXT) && !node.equals(scope))
                            .map(node -> node.hasRelationship(Components.FITS1, Direction.INCOMING))
                            .reduce(true, (aBoolean, aBoolean2) -> aBoolean && aBoolean2));

            Assert.assertTrue(
                    ((ResourceIterator<Relationship>) emptyContext
                                .getRelationships(Components.FITS1, Direction.OUTGOING).iterator())
                            .stream().count() == 13);
                                // includes other contexts!

            Assert.assertTrue(
                    ((ResourceIterator<Relationship>) emptyContext2
                                .getRelationships(Components.FITS1, Direction.OUTGOING).iterator())
                            .stream().count() == 0);

//            tx.acquireWriteLock(emptyContext);

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
            SCLabeler scLabeler = comp.getLabeler();

            // load test graph
            db.execute(TestGraphQueries.systemsWithShapeProperties);

            // view graph state
            System.out.println(db.execute(TestGraphQueries.viewGraph).resultAsString());

            // label all fits
            scLabeler.labelAllFits();

            // label all ready
            scLabeler.labelAllReady();

            // view and compare graph state
            Result result = db.execute(TestGraphQueries.viewGraph);
            System.out.println(result.resultAsString());
            Assert.assertEquals(
                    "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                            "| ID | Labels                       | Properties                                                                              | Relationships                 |\n" +
                            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                            "| 0  | [\"System\",\"Data\",\"Data1\"]    | {name -> \"a1\", data -> 10}                                                              | {r -> <null>, n -> <null>}    |\n" +
                            "| 1  | [\"System\",\"Data\",\"Data2\"]    | {name -> \"a2\", data -> 8}                                                               | {r -> <null>, n -> <null>}    |\n" +
                            "| 2  | [\"System\",\"Data\",\"Data1\"]    | {name -> \"a3\", data -> 9}                                                               | {r -> <null>, n -> <null>}    |\n" +
                            "| 3  | [\"System\",\"Data\",\"Data2\"]    | {name -> \"a4\", data -> 6}                                                               | {r -> <null>, n -> <null>}    |\n" +
                            "| 4  | [\"System\",\"SCOPE\"]           | {name -> \"main\"}                                                                        | {r -> :CONTAINS[0]{}, n -> 7} |\n" +
                            "| 4  | [\"System\",\"SCOPE\"]           | {name -> \"main\"}                                                                        | {r -> :CONTAINS[1]{}, n -> 6} |\n" +
                            "| 4  | [\"System\",\"SCOPE\"]           | {name -> \"main\"}                                                                        | {r -> :CONTAINS[2]{}, n -> 8} |\n" +
                            "| 4  | [\"System\",\"SCOPE\"]           | {name -> \"main\"}                                                                        | {r -> :CONTAINS[3]{}, n -> 9} |\n" +
                            "| 5  | [\"System\",\"CONTEXT\",\"READY\"] | {name -> \"subE\", function -> \"SUBTRACTe\", s1Labels -> [\"Data1\"], s2Labels -> [\"Data2\"]} | {r -> :FITS1[10]{}, n -> 0}   |\n" +
                            "| 5  | [\"System\",\"CONTEXT\",\"READY\"] | {name -> \"subE\", function -> \"SUBTRACTe\", s1Labels -> [\"Data1\"], s2Labels -> [\"Data2\"]} | {r -> :FITS1[12]{}, n -> 2}   |\n" +
                            "| 5  | [\"System\",\"CONTEXT\",\"READY\"] | {name -> \"subE\", function -> \"SUBTRACTe\", s1Labels -> [\"Data1\"], s2Labels -> [\"Data2\"]} | {r -> :FITS2[11]{}, n -> 1}   |\n" +
                            "| 5  | [\"System\",\"CONTEXT\",\"READY\"] | {name -> \"subE\", function -> \"SUBTRACTe\", s1Labels -> [\"Data1\"], s2Labels -> [\"Data2\"]} | {r -> :FITS2[13]{}, n -> 3}   |\n" +
                            "| 6  | [\"System\",\"CONTEXT\"]         | {name -> \"mul\", function -> \"MULTIPLY\", s1Labels -> [\"Data\"], s2Labels -> [\"Data\"]}     | {r -> <null>, n -> <null>}    |\n" +
                            "| 7  | [\"System\",\"CONTEXT\"]         | {name -> \"print\", function -> \"PRINT\", s1Labels -> [\"Data\"], s2Labels -> [\"Data\"]}      | {r -> <null>, n -> <null>}    |\n" +
                            "| 8  | [\"System\",\"SCOPE\"]           | {name -> \"c1\"}                                                                          | {r -> :CONTAINS[4]{}, n -> 5} |\n" +
                            "| 8  | [\"System\",\"SCOPE\"]           | {name -> \"c1\"}                                                                          | {r -> :CONTAINS[5]{}, n -> 0} |\n" +
                            "| 8  | [\"System\",\"SCOPE\"]           | {name -> \"c1\"}                                                                          | {r -> :CONTAINS[6]{}, n -> 1} |\n" +
                            "| 9  | [\"System\",\"SCOPE\"]           | {name -> \"c2\"}                                                                          | {r -> :CONTAINS[7]{}, n -> 5} |\n" +
                            "| 9  | [\"System\",\"SCOPE\"]           | {name -> \"c2\"}                                                                          | {r -> :CONTAINS[8]{}, n -> 2} |\n" +
                            "| 9  | [\"System\",\"SCOPE\"]           | {name -> \"c2\"}                                                                          | {r -> :CONTAINS[9]{}, n -> 3} |\n" +
                            "+-------------------------------------------------------------------------------------------------------------------------------------------------------------+\n" +
                            "20 rows\n", result.resultAsString());

        }
    }
}