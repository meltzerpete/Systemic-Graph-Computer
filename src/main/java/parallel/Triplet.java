package parallel;

import org.neo4j.graphdb.Node;

import java.util.function.BiConsumer;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class Triplet {

    long context;
    long s1;
    long s2;
    BiConsumer<Node, Node> function;

    public Triplet(long context, long s1, long s2, BiConsumer<Node, Node> function) {
        this.context = context;
        this.s1 = s1;
        this.s2 = s2;
        this.function = function;
    }

    @Override
    public String toString() {
        return context + " " + s1 + ", " + s2;
    }
}
