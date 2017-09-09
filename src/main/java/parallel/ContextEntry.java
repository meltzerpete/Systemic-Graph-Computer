package parallel;

import nodeParser.NodeMatch;
import org.neo4j.graphdb.Node;

import java.util.function.BiConsumer;

/**
 * Created by Pete Meltzer on 06/08/17.
 * <p>A class used for aching all details of a context system
 * including the function, the contextID, the scopeID, and a pair
 * of NodeMatch objects required for matching.</p>
 */
public class ContextEntry {
    long context;
    BiConsumer<Node, Node> function;
    NodeMatch s1;
    NodeMatch s2;
    long scope;

    /**
     * @param context the context ID
     * @param function the transformation function
     * @param s1 the s1 NodeMatch object
     * @param s2 the s2 NodeMatch object
     * @param scope the scope ID
     */
    public ContextEntry(long context, BiConsumer<Node, Node> function, NodeMatch s1, NodeMatch s2, long scope) {
        this.context = context;
        this.function = function;
        this.s1 = s1;
        this.s2 = s2;
        this.scope = scope;
    }
}
