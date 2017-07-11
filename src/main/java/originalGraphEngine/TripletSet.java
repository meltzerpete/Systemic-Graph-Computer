package originalGraphEngine;

import org.neo4j.graphdb.Node;

import java.util.HashSet;

/**
 * Created by pete on 03/07/17.
 */
public class TripletSet {

    public Node context;
    public HashSet<Node> f1;
    public HashSet<Node> f2;

    TripletSet(Node context, HashSet<Node> f1, HashSet<Node> f2) {
        this.context = context;
        this.f1 = f1;
        this.f2 = f2;
    }
}
