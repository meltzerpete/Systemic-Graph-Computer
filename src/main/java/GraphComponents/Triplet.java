package GraphComponents;

import org.neo4j.graphdb.Node;

/**
 * Created by pete on 09/07/17.
 */
public class Triplet {
    public Node context;
    public Node s1;
    public Node s2;

    public Triplet(Node c, Node s1, Node s2) {
        context = c;
        this.s1 = s1;
        this.s2 = s2;
    }

    @Override
    public String toString() {
        return "{" + context.getProperty("name") + ": " + context.getProperty("function") + "}, " +
                "{" + s1.getProperty("name") + ": " + s1.getProperty("data") + "}, " +
                "{" + s2.getProperty("name") + ": " + s2.getProperty("data") + "}, ";
    }
}
