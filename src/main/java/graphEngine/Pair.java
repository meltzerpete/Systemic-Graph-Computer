package graphEngine;

import org.neo4j.graphdb.Node;

import java.util.stream.Stream;

/**
 * Created by Pete Meltzer on 15/07/17.
 * <p>Composition of a pair of Nodes.</p>
 */
public class Pair {
    Node s1;
    Node s2;

    @Override
    public String toString() {
        String s1String = s1 == null ? "null" : s1.getProperty("key").toString();
        String s2String = s2 == null ? "null" : s2.getProperty("key").toString();
        return "[s1: " + s1String + ", s2: " + s2String + "]";
    }

    /**
     * Returns the pair of Nodes as a stream.
     * @return Pair of Nodes as a Stream
     */
    public Stream<Node> getAll() {
        return Stream.of(s1, s2);
    }
}
