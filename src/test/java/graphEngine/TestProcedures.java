package graphEngine;

import org.neo4j.graphdb.*;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class TestProcedures {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.SCSystemHandlerTest", mode = Mode.SCHEMA)
    public void test(@Name("MethodID") long methodID) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        db.execute(TestGraphQueries.systemsWithShapeProperties);
        SCSystemHandler handler = new Computer(db).getHandler();

        switch ((int) methodID) {
            case 0:
                System.out.println("getAllSystemsInScope()");
                db.findNodes(Components.SCOPE).forEachRemaining(node -> {
                    System.out.println("------------------------------------------------------------");
                    System.out.println(
                            "In scope " + node.getProperty("name"));
                    Stream<Node> nodes = handler.getAllSystemsInScope(node);
                    nodes.forEach(node1 ->
                    System.out.println(node1.getProperty("name")));
                });
                System.out.println("============================================================");
                break;

            case 1:
                System.out.println("getContextsInScope()");
                db.findNodes(Components.SCOPE).forEachRemaining(node -> {
                    System.out.println("------------------------------------------------------------");
                    System.out.println(
                            "In scope " + node.getProperty("name"));
                    Stream<Node> nodes = handler.getContextsInScope(node);
                    nodes.forEach(node1 ->
                            System.out.println(node1.getProperty("name")));
                });
                System.out.println("============================================================");
                break;

            case 2:
                System.out.println("getRandomEndNode()");

                Method getRandomEndNode = SCSystemHandler.class.getDeclaredMethod("getRandomEndNode", Stream.class);
                getRandomEndNode.setAccessible(true);

                db.findNodes(Components.SCOPE).forEachRemaining(node -> {
                    System.out.println("------------------------------------------------------------");
                    System.out.println(
                            "In scope " + node.getProperty("name"));
                    for (int i = 0; i < 10; i++) {
                        ResourceIterator<Relationship> rels =
                                (ResourceIterator<Relationship>) node.getRelationships(Components.CONTAINS, Direction.OUTGOING).iterator();

                        try {
                            Node end = (Node) getRandomEndNode.invoke(handler, rels.stream());
                            System.out.println(end.getProperty("name"));
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (InvocationTargetException e) {
                            e.printStackTrace();
                        }

                    }
                });
                System.out.println("============================================================");
                break;

            case 3:
                System.out.println("getRandomNode()");

                Method getRandomNode = SCSystemHandler.class.getDeclaredMethod("getRandomNode", Stream.class);
                getRandomNode.setAccessible(true);

                for (int i = 0; i < 20; i++) {
                    Node randNode = (Node) getRandomNode.invoke(handler, db.getAllNodes().stream());
                    System.out.println(randNode.getProperty("name"));
                }
                System.out.println("============================================================");
                break;

            case 4:
                System.out.println("getRandomReady()");
                System.out.println("------------------------------------------------------------");
                for (int i = 0; i < 20; i++) {
                    Node randReady = handler.getRandomReady();
                    if (!randReady.hasLabel(Components.READY))
                        throw new AssertionError("FAIL");
                    System.out.println(randReady.getProperty("name"));
                }
                System.out.println("============================================================");
                break;
        }


    }
}
