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
    public void getInstance() throws Throwable {
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

            String result = db.execute("MATCH (n)" +
                    "OPTIONAL MATCH (n)-[r]->(m)" +
                    "RETURN DISTINCT id(n) AS ID, labels(n) AS Labels," +
                    "properties(n) AS Properties, {r:r, n:id(m)} AS Relationships").resultAsString();
            System.out.println(result);

            scLabeler.labelFitsInScope(scope);

            result = db.execute("MATCH (n)" +
                    "OPTIONAL MATCH (n)-[r]->(m)" +
                    "RETURN DISTINCT id(n) AS ID, labels(n) AS Labels," +
                    "properties(n) AS Properties, {r:r, n:id(m)} AS Relationships").resultAsString();
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
}