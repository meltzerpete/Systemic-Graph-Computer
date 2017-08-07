package parallel;

import nodeParser.NodeMatch;
import org.neo4j.graphdb.Node;

import java.util.function.BiConsumer;

/**
 * Created by Pete Meltzer on 06/08/17.
 */
public class ContextEntry {
    long context;
    BiConsumer<Node, Node> contextFunction;
    NodeMatch s1;
    NodeMatch s2;
    long scope;

    public ContextEntry(long context, BiConsumer<Node, Node> contextFunction, NodeMatch s1, NodeMatch s2, long scope) {
        this.context = context;
        this.contextFunction = contextFunction;
        this.s1 = s1;
        this.s2 = s2;
        this.scope = scope;
    }
}
