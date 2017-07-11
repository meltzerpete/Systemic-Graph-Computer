package originalGraphEngine;

import org.neo4j.graphdb.*;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by pete on 06/07/17.
 */
@Deprecated
public class Matching {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "originalGraphEngine.findMatches", mode = Mode.SCHEMA)
    @Description("")
    public void findMatches() {

        Label contextLabel = Label.label("Context");
        Label scopeLabel = Label.label("Scope");
        Label shapeLabel = Label.label("Shape");
        RelationshipType contains = RelationshipType.withName("CONTAINS");
        RelationshipType hasShape = RelationshipType.withName("HAS_SHAPE");
        RelationshipType s1 = RelationshipType.withName("S1");
        RelationshipType s2 = RelationshipType.withName("S2");

        ResourceIterator<Node> scopes = db.findNodes(scopeLabel);

        // in every scope
        scopes.forEachRemaining(scope -> {

            List<SCSystem> contexts = new LinkedList<>();
            List<SCSystem> children = new LinkedList<>();

            // create list of contexts with corresponding s1 and s2 set
            Iterables.stream(scope.getRelationships(contains, Direction.OUTGOING))
                    .filter(relationship ->
                       relationship.getEndNode().hasLabel(contextLabel))
                    .forEach(relationship -> {
                           SCSystem context = new SCSystem();
                           context.node = relationship.getEndNode();

                           // add each shape to s1
                           context.node.getRelationships(s1, Direction.OUTGOING)
                                   .forEach(relationship1 ->
                                           context.s1.add(relationship1.getEndNode()));

                           // add each shape to s2;
                           context.node.getRelationships(s2, Direction.OUTGOING)
                                   .forEach(relationship1 ->
                                            context.s2.add(relationship1.getEndNode()));

                           contexts.add(context);
                    });
            System.out.println("===================================================================");
            System.out.println("Scope: " + scope.getAllProperties());
            System.out.println("===================================================================");
            System.out.println("Contexts:");
            contexts.forEach(scSystem -> System.out.println(scSystem));
            System.out.println("-------------------------------------------------------------------");
            System.out.println("All child systems:");
            System.out.println("-------------------------------------------------------------------\n");
        });

    }

    private class SCSystem {
        private Node node;
        private HashSet<Node> s1;
        private HashSet<Node> s2;
        private HashSet<Node> shape;

        {
            s1 = new HashSet<>();
            s2 = new HashSet<>();
            shape = new HashSet<>();
        }

        @Override
        public String toString() {
            String out = node == null ? "null" : node.getAllProperties().toString();
            out += s1 == null ? "" : ", s1: " + s1.toString();
            out += s2 == null ? "" : ", s2: " + s2.toString();
            out += shape == null ? "" : ", shape: " + shape.toString();
            return out;
        }
    }
}
