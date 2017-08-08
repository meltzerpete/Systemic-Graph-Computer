package parallel;

import graphEngine.Components;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * Created by Pete Meltzer on 06/08/17.
 */
public class Functions {

    static ReentrantLock scopeVectorsLock = new ReentrantLock();

    // extra labels
    static final Label INITIALIZED = Label.label("Initialized");
    static final Label UNINITIALIZED = Label.label("Uninitialized");

    static int W = 80;
    static int[] w = {15,20,1,3,8,2,16,17,11,19,10,5,18,4,7,9};
    static int[] v = {20,14,12,9,5,17,7,6,1,11,4,19,13,2,15,3};

    public static BiConsumer<Node, Node> getFunction(String functionName) {
        switch (functionName) {
            case "INITIALIZE": return initialize;
            case "BINARYMUTATE": return binaryMutate;
            case "ONEPOINTCROSS": return onePointCross;
            case "UNIFORMCROSS": return uniformCross;
            case "OUTPUT": return output;
            //TODO
            default: throw new AssertionError("Invalid Function Code");
        }
    }

    static BiConsumer<Node, Node> initialize = (s1, s2) -> {
        // create random char for uninitialized system
        char randomChar = (char) (Math.random() * Math.pow(2, 16));
        char x = guard(randomChar);
        s1.setProperty(Components.data, x);

        // insert initialized system into s2 scope
        addToScope(s2, s1);

        // change label to initialized
        s1.removeLabel(UNINITIALIZED);
        s1.addLabel(INITIALIZED);
    };

    static BiConsumer<Node, Node> binaryMutate = (s1, s2) -> {
        for (Node s : new Node[]{s1, s2}) {

            // flip random bit
            char bitMask = (char) (Math.pow(2, (int) (Math.random() * 16)));

            char originalChar = (char) s.getProperty(Components.data);
            char newChar = (char) (originalChar ^ bitMask);

            s.setProperty(Components.data, guard(newChar));
        }
    };

    static BiConsumer<Node, Node> onePointCross = (s1, s2) -> {
        char p1 = (char) s1.getProperty(Components.data);
        char p2 = (char) s2.getProperty(Components.data);

        int position = (int) (Math.random() * 16);

        char bitMaskA = (char) (0xffff >>> position);
        char bitMaskB = (char) (bitMaskA ^ 0xffff);

        char c1 = (char) ((p1 & bitMaskA) | (p2 & bitMaskB));
        char c2 = (char) ((p1 & bitMaskB) | (p2 & bitMaskA));

        s1.setProperty(Components.data, guard(c1));
        s2.setProperty(Components.data, guard(c2));
    };

    static BiConsumer<Node, Node> uniformCross = (s1, s2) -> {
        char p1 = (char) s1.getProperty(Components.data);
        char p2 = (char) s2.getProperty(Components.data);

        char bitMaskA = (char) (Math.random() * Math.pow(2, 16));
        char bitMaskB = (char) (bitMaskA ^ 0xffff);

        char c1 = (char) ((p1 & bitMaskA) | (p2 & bitMaskB));
        char c2 = (char) ((p1 & bitMaskB) | (p2 & bitMaskA));

        s1.setProperty(Components.data, guard(c1));
        s2.setProperty(Components.data, guard(c2));
    };

    static BiConsumer<Node, Node> output = (s1, s2) -> {
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
        }
    };

    private static int fitness(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += v[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    private static int weight(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += w[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    private static char guard(char x) {

        //TODO random or systematic?
        while (weight(x) > W) {

            char bitMask = (char) (0xffff ^ ((char) (0x0001 << (int) (Math.random() * 16))));
            x &= bitMask;

        }
        return x;
    }

    //TODO function to change scope of node
//    private static void removeFromScope(Node scope, Node node) {
//        scope.get
//    }

    private static void addToScope(Node scope, Node node) {
        scope.createRelationshipTo(node, Components.CONTAINS);
        long scopeID = scope.getId();

        // critical section
        scopeVectorsLock.lock();
        try {
            Long[] oldArray = Single.scopeContainedIDs.get(scopeID);
            Long[] newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
            newArray[oldArray.length] = node.getId();
            Single.scopeContainedIDs.replace(scopeID, newArray);
        } finally {
            scopeVectorsLock.unlock();
        }
    }
}
