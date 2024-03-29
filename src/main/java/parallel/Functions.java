package parallel;

import common.Components;
import org.neo4j.graphdb.Node;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;

/**
 * Created by Pete Meltzer on 06/08/17.
 * <p>All transformations functions are stored here.</p>
 * <p>All details for the Knapsack problem are also set here.
 * W is the maximum wieght, w the array of all pack weights, and
 * v the corresponding array of all pack values.</p>
 */
public class Functions {

    private final Manager manager;

    /**
     * @param manager Instance of the Manager class
     */
    public Functions(Manager manager) {
        this.manager = manager;
    }

    int W = 80;
    int[] w = {15,20,1,3,8,2,16,17,11,19,10,5,18,4,7,9};
    int[] v = {20,14,12,9,5,17,7,6,1,11,4,19,13,2,15,3};

    /**
     * Determines the function from the string represntation stored in
     * a context Node.
     * @param functionName string representation of function name
     * @return BiConsumer of the function
     */
    public BiConsumer<Node, Node> getFunction(String functionName) {

        switch (functionName) {
            case "INITIALIZE": return initialize;
            case "BINARYMUTATE": return binaryMutate;
            case "ONEPOINTCROSS": return onePointCross;
            case "UNIFORMCROSS": return uniformCross;
            case "OUTPUT": return output;
            default: throw new AssertionError("Invalid Function Code");
        }
    }

    BiConsumer<Node, Node> initialize = (s1, s2) -> {

        // create random char for uninitialized system
        char randomChar = (char) (ThreadLocalRandom.current().nextInt((int) Math.pow(2, 16)));
        char x = guard(randomChar);
        s1.setProperty(Components.data, x);

        // insert initialized system into s2 scope
        addToScope(s2, s1);

        // change label to initialized
        s1.removeLabel(Components.UNINITIALIZED);
        s1.addLabel(Components.INITIALIZED);
    };

    BiConsumer<Node, Node> binaryMutate = (s1, s2) -> {

        for (Node s : new Node[]{s1, s2}) {

            // flip random bit
            char bitMask = (char) (Math.pow(2, ThreadLocalRandom.current().nextInt(16)));

            char originalChar = (char) s.getProperty(Components.data);
            char newChar = (char) (originalChar ^ bitMask);

            s.setProperty(Components.data, guard(newChar));
        }
    };

    BiConsumer<Node, Node> onePointCross = (s1, s2) -> {

        char p1 = (char) s1.getProperty(Components.data);
        char p2 = (char) s2.getProperty(Components.data);

        int position = ThreadLocalRandom.current().nextInt(16);

        char bitMaskA = (char) (0xffff >>> position);
        char bitMaskB = (char) (bitMaskA ^ 0xffff);

        char c1 = (char) ((p1 & bitMaskA) | (p2 & bitMaskB));
        char c2 = (char) ((p1 & bitMaskB) | (p2 & bitMaskA));

        s1.setProperty(Components.data, guard(c1));
        s2.setProperty(Components.data, guard(c2));
    };

    BiConsumer<Node, Node> uniformCross = (s1, s2) -> {

        char p1 = (char) s1.getProperty(Components.data);
        char p2 = (char) s2.getProperty(Components.data);

        char bitMaskA = (char) (ThreadLocalRandom.current().nextInt(16));
        char bitMaskB = (char) (bitMaskA ^ 0xffff);

        char c1 = (char) ((p1 & bitMaskA) | (p2 & bitMaskB));
        char c2 = (char) ((p1 & bitMaskB) | (p2 & bitMaskA));

        s1.setProperty(Components.data, guard(c1));
        s2.setProperty(Components.data, guard(c2));
    };

    BiConsumer<Node, Node> output = (s1, s2) -> {

        if (!s1.hasProperty(Components.data))
            s1.setProperty(Components.data, (char) 0x0000);

        char fittest = (char) s1.getProperty(Components.data);
        char other = (char) s2.getProperty(Components.data);

        if (fitness(other) > fitness(fittest)) {
            s1.setProperty(Components.data, other);
            System.out.println(String.format(
                    "Fittest solution (compare against %d) - weight: %d, value: %d, solution: %s",
                    fitness(fittest), weight(other), fitness(other), Integer.toBinaryString(other)
            ));
        } else {
            s2.setProperty(Components.data, fittest);
        }
    };

    private int fitness(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += v[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    private int weight(char x) {

        int value = 0;
        char bitMask = 0x8000;

        for (int i = 0; i < 16; i++) {
            if ((x & bitMask) != 0) value += w[i];
            bitMask = (char) (bitMask >>> 1);
        }

        return value;
    }

    private char guard(char x) {

        while (weight(x) > W) {
            char bitMask = (char) (0xffff ^ ((char) (0x0001 << ThreadLocalRandom.current().nextInt( 16))));
            x &= bitMask;
        }

        return x;
    }

    private void addToScope(Node scope, Node node) {

        scope.createRelationshipTo(node, Components.CONTAINS);
        long scopeID = scope.getId();
        long nodeID = node.getId();

        Long[] oldArray = manager.nodesContainedInScope.get(scopeID);
        Long[] newArray = Arrays.copyOf(oldArray, oldArray.length + 1);
        newArray[oldArray.length] = nodeID;
        manager.nodesContainedInScope.replace(scopeID, newArray);
    }
}
