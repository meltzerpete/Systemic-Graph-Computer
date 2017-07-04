package graphEngine;

import org.neo4j.graphdb.Node;

/**
 * Created by pete on 03/07/17.
 */
public class Triplet {

    public Node context;
    public Node f1;
    public Node f2;

    Triplet(Node context, Node f1, Node f2) {
        this.context = context;
        this.f1 = f1;
        this.f2 = f2;
    }
}
