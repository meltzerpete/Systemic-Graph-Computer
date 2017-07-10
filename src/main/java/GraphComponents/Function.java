package GraphComponents;

import graphEngine.Compute;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by pete on 04/07/17.
 */
public enum Function {
    NOP {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing NOP on " + triplet);
        }
    },
    ADD {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing ADD on " + triplet);
            long a = (long) triplet.s1.getProperty("data");
            long b = (long) triplet.s2.getProperty("data");
            long c = a + b;
            triplet.s1.setProperty("data", c);
            triplet.s2.setProperty("data", (long) 0);
            System.out.println("State is now " + triplet);
        }
    },
    ADDe {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing ADDe on " + triplet);
            ADD.compute(triplet);
            ESCAPE.compute(triplet);
        }
    },
    SUBTRACT {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing SUBTRACT on " + triplet);
            long a = (long) triplet.s1.getProperty("data");
            long b = (long) triplet.s2.getProperty("data");
            long c = a - b;
            triplet.s1.setProperty("data", c);
            triplet.s2.setProperty("data", (long) 0);
            System.out.println("State is now " + triplet);
        }
    },
    SUBTRACTe {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing SUBTRACTe on " + triplet);
            SUBTRACT.compute(triplet);
            ESCAPE.compute(triplet);
        }
    },
    MULTIPLY {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing MULTIPLY on " + triplet);
            long a = (long) triplet.s1.getProperty("data");
            long b = (long) triplet.s2.getProperty("data");
            long c = a * b;
            triplet.s1.setProperty("data", c);
            triplet.s2.setProperty("data", (long) 1);
        }
    },
    MULTIPLYe {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing MULTIPLYe on " + triplet);
            MULTIPLY.compute(triplet);
            ESCAPE.compute(triplet);
        }
    },
    ESCAPE {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing ESCAPE on " + triplet);
            Node s1 = triplet.s1;

            List<Node> parentScopes = new LinkedList<>();
            s1.getRelationships(Components.contains, Direction.INCOMING).forEach(relationship -> {
                if (relationship.getStartNode().hasLabel(Components.scopeLabel)) {
                    parentScopes.add(relationship.getStartNode());
                    relationship.delete();
                }
            });

            Node context = triplet.context;

            context.removeLabel(Components.readyLabel);

            s1.getRelationships(Components.fits1, Direction.INCOMING).forEach(relationship ->
                    relationship.delete());

            s1.getRelationships(Components.fits2, Direction.INCOMING).forEach((relationship ->
                    relationship.delete()));

            parentScopes.forEach(parent -> {
                parent.getRelationships(Components.contains, Direction.INCOMING).forEach(relationship -> {
                    relationship.getStartNode().createRelationshipTo(s1, Components.contains);
                });
            });


        }
    },
    PRINT {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing PRINT function on " + triplet);
            System.out.println("\n########################################");
            System.out.println(String.format("#%-38s#", triplet.s1.getProperty("data")));
            System.out.println("########################################\n");
            Compute.outputStream.add(new Compute.Output("From " + triplet + ": " + triplet.s1.getProperty("data")));
        }
    };

    public abstract void compute(Triplet triplet);

}
