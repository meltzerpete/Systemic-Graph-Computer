package graphEngine;


import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;

import java.util.Locale;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
enum Function {

    NOP {
        @Override
        void perform(Node context, Node s1, Node s2) {
            //TODO NOP()
        }
    },

    ADD {
        @Override
        void perform(Node context, Node s1, Node s2) {

            System.out.println("ADD");
            long result = ((long) s1.getProperty("data")) + ((long) s2.getProperty("data"));
            s1.setProperty("data", result);
            s2.getProperty("data", ((long) 0));
        }
    },
    ESCAPE {
        @Override
        boolean affectsS1parentScopes() {
            return true;
        }

        @Override
        void perform(Node context, Node s1, Node s2) {
            //TODO ESCAPE()

//            System.out.println("ESCAPE on: " + s1.getProperty("key"));
            ResourceIterator<Relationship> containsRelationships =
                    (ResourceIterator<Relationship>) s1.getRelationships(Components.CONTAINS, Direction.INCOMING).iterator();

            containsRelationships.stream()
                    .forEach(rel -> {
                        Node scope = rel.getStartNode();
                        rel.delete();

                        ResourceIterator<Relationship> parentScopesRelationships =
                                (ResourceIterator<Relationship>) scope.getRelationships(Components.CONTAINS, Direction.INCOMING).iterator();

                        parentScopesRelationships.stream()
                                .map(Relationship::getStartNode)
                                .forEach(parentScope -> parentScope.createRelationshipTo(s1, Components.CONTAINS));
                    });
        }
    },
    MULTIPLY {
        @Override
        void perform(Node context, Node s1, Node s2) {

//            System.out.println("MULTIPLY on: " + s1.getProperty("key") + " and " + s2.getProperty("key"));
            long result = ((long) s1.getProperty("data")) * ((long) s2.getProperty("data"));
            s1.setProperty("data", result);
            s2.setProperty("data", ((long) 1));
        }
    },
    PRINT {
        @Override
        void perform(Node context, Node s1, Node s2) {

            //TODO swap for adding to return output
            System.out.println(String.format(Locale.UK,
                        "********************PRINT********************\n" +
                                "* s1: %-38s*\n" +
                                "* s2: %-38s*\n" +
                                "*********************************************",
                    s1.getProperty("data"), s2.getProperty("data")));

//            System.out.println("PRINT (s1): " + s1.getAllProperties());
//            System.out.println("PRINT (s2): " + s2.getAllProperties());
        }
    },
    SUBTRACT {
        @Override
        void perform(Node context, Node s1, Node s2) {

//            System.out.println("SUBTRACT");
            long result = ((long) s1.getProperty("data")) - ((long) s2.getProperty("data"));
            s1.setProperty("data", result);
            s2.setProperty("data", ((long) 0));
        }
    },
    SUBTRACTe {
        @Override
        boolean affectsS1parentScopes() {
            return true;
        }

        @Override
        void perform(Node context, Node s1, Node s2) {

            SUBTRACT.perform(context, s1, s2);
            ESCAPE.perform(context, s1, s2);
        }
    };

    //TODO add remaining functions

    abstract void perform(Node context, Node s1, Node s2);

    boolean affectsS1parentScopes() {
        return false;
    }

    boolean affectsS2parentScopes() {
        return false;
    }
}