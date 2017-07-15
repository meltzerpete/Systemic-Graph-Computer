package graphEngine;

import org.neo4j.graphdb.Node;

/**
 * Created by Pete Meltzer on 15/07/17.
 */
public class Pair {
    Node s1;
    Node s2;

    @Override
    public String toString() {
        String s1String = s1 == null ? "null" : s1.getProperty("name").toString();
        String s2String = s2 == null ? "null" : s2.getProperty("name").toString();
        return "[s1: " + s1String + ", s2: " + s2String + "]";
    }
}
