package graphEngine;


import common.Components;
import org.neo4j.graphdb.*;

import java.util.Locale;

/**
 * Created by Pete Meltzer on 11/07/17.
 * <p>All transformations functions are stored here.</p>
 * <p>All details for the Knapsack problem are also set here.
 * W is the maximum wieght, w the array of all pack weights, and
 * v the corresponding array of all pack values.</p>
 * <p>Each enum entry must provide implementation for the perform
 * function. If the function affects either s1 or s2's parent scope
 * membership, the affectsS1parentScope() and/or
 * affectsS2parentScope() must also be overriden to return {@code true}.</p>
 */
enum Function {

    NOP {
        @Override
        void perform(Node context, Node s1, Node s2) {
            //do nothing
        }
    },

    ADD {
        @Override
        void perform(Node context, Node s1, Node s2) {

            System.out.println("ADD");
            long result = ((long) s1.getProperty(Components.data)) + ((long) s2.getProperty(Components.data));
            s1.setProperty(Components.data, result);
            s2.getProperty(Components.data, ((long) 0));
        }
    },
    ESCAPE {
        @Override
        boolean affectsS1parentScopes() {
            return true;
        }

        @Override
        void perform(Node context, Node s1, Node s2) {

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
            long result = ((long) s1.getProperty(Components.data)) * ((long) s2.getProperty(Components.data));
            s1.setProperty(Components.data, result);
            s2.setProperty(Components.data, ((long) 1));
        }
    },
    PRINT {
        @Override
        void perform(Node context, Node s1, Node s2) {

            System.out.println(String.format(Locale.UK,
                        "********************PRINT********************\n" +
                                "* s1: %-38s*\n" +
                                "* s2: %-38s*\n" +
                                "*********************************************",
                    s1.getProperty(Components.data), s2.getProperty(Components.data)));

//            System.out.println("PRINT (s1): " + s1.getAllProperties());
//            System.out.println("PRINT (s2): " + s2.getAllProperties());
        }
    },
    SUBTRACT {
        @Override
        void perform(Node context, Node s1, Node s2) {

//            System.out.println("SUBTRACT");
            long result = ((long) s1.getProperty(Components.data)) - ((long) s2.getProperty(Components.data));
            s1.setProperty(Components.data, result);
            s2.setProperty(Components.data, ((long) 0));
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
    },
    INITIALIZE {
        @Override
        void perform(Node context, Node s1, Node s2) {

            // create random char for uninitialized system
            char randomChar = (char) (Math.random() * Math.pow(2, 16));
            char x = guard(randomChar);
            s1.setProperty(Components.data, x);

            // insert initialized system into s2 scope
            s2.createRelationshipTo(s1, Components.CONTAINS);

            // change label to initialized
            s1.removeLabel(Components.UNINITIALIZED);
            s1.addLabel(Components.INITIALIZED);

        }
    },
    BINARYMUTATE {
        @Override
        void perform(Node context, Node s1, Node s2) {

            for (Node s : new Node[]{s1, s2}) {

                // flip random bit
                char bitMask = (char) (Math.pow(2, (int) (Math.random() * 16)));

                char originalChar = (char) s.getProperty(Components.data);
                char newChar = (char) (originalChar ^ bitMask);

                s.setProperty(Components.data, guard(newChar));
            }

        }
    },
    ONEPOINTCROSS {
        @Override
        void perform(Node context, Node s1, Node s2) {

            char p1 = (char) s1.getProperty(Components.data);
            char p2 = (char) s2.getProperty(Components.data);

            int position = (int) (Math.random() * 16);

            char bitMaskA = (char) (0xffff >>> position);
            char bitMaskB = (char) (bitMaskA ^ 0xffff);

            char c1 = (char) ((p1 & bitMaskA) | (p2 & bitMaskB));
            char c2 = (char) ((p1 & bitMaskB) | (p2 & bitMaskA));

            s1.setProperty(Components.data, guard(c1));
            s2.setProperty(Components.data, guard(c2));
        }
    },
    UNIFORMCROSS {
        @Override
        void perform(Node context, Node s1, Node s2) {

            char p1 = (char) s1.getProperty(Components.data);
            char p2 = (char) s2.getProperty(Components.data);

            char bitMaskA = (char) (Math.random() * Math.pow(2, 16));
            char bitMaskB = (char) (bitMaskA ^ 0xffff);

            char c1 = (char) ((p1 & bitMaskA) | (p2 & bitMaskB));
            char c2 = (char) ((p1 & bitMaskB) | (p2 & bitMaskA));

            s1.setProperty(Components.data, guard(c1));
            s2.setProperty(Components.data, guard(c2));
        }
    },
    OUTPUT {
        @Override
        void perform(Node context, Node s1, Node s2) {

            if (!s1.hasProperty(Components.data))
                s1.setProperty(Components.data, (char) 0x0000);

            char fittest = (char) s1.getProperty(Components.data);
            char other = (char) s2.getProperty(Components.data);

            if (fitness(other) > fitness(fittest)) {
                s1.setProperty(Components.data, other);
                System.out.println(String.format(
                        "Fittest solution - weight: %d, value: %d, solution: %s",
                        weight(other), fitness(other), Integer.toBinaryString(other)
                ));
            } else {
                s2.setProperty(Components.data, fittest);
            }
        }
    };

    /**
     * All functions added as enum entries must override this method
     * to provide an implementation.
     * @param context the context Node
     * @param s1 s1 Node
     * @param s2 s2 Node
     */
    // all functions must provide an implementation of the perform method according to the following contract
    abstract void perform(Node context, Node s1, Node s2);

    /**
     * Override this method if the transformation function affects
     * the parent scope of S1.
     * @return true if scope membership affected, false if not
     */
    // any function that changes the parent scope membership of either system must override either/both of the following to return true
    boolean affectsS1parentScopes() {
        return false;
    }

    /**
     * Override this method if the transformation function affects
     * the parent scope of S2.
     * @return true if scope membership affected, false if not
     */
    boolean affectsS2parentScopes() {
        return false;
    }

    int W = 80;
    int[] w = {15,20,1,3,8,2,16,17,11,19,10,5,18,4,7,9};
    int[] v = {20,14,12,9,5,17,7,6,1,11,4,19,13,2,15,3};

    /**
     * Calculates the fitness (i.e. the value) of the solution.
     * @param x the solution
     * @return value
     */
    int fitness(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += v[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    /**
     * Caculates the weight of a solution.
     * @param x the solution
     * @return weight
     */
    int weight(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += w[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    /**
     * Randomly removes packs from the solution until the
     * weight is less than or equal to the maximum
     * @param x potentially illegal solution
     * @return legal solution
     */
    char guard(char x) {

        while (weight(x) > W) {

            char bitMask = (char) (0xffff ^ ((char) (0x0001 << (int) (Math.random() * 16))));
            x &= bitMask;

        }
        return x;
    }


}