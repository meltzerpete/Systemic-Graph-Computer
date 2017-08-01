package queryCompiler;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

/**
 * Created by Pete Meltzer on 25/07/17.
 */
public class Vertex {

    @SuppressWarnings("unchecked cast")
    public LinkedList<Label> getLabels() {
        return (LinkedList<Label>) labels.clone();
    }

    @SuppressWarnings("unchecked cast")
    public LinkedList<PropertyPair<Object>> getProperties() {
        return (LinkedList<PropertyPair<Object>>) properties.clone();
    }

    @SuppressWarnings("unchecked cast")
    public LinkedList<Edge> getEdges() {
        return (LinkedList<Edge>) edges.clone();
    }

    public int getDepth() { return getDepth(this, new HashSet<>()); }

    private int getDepth(Vertex v, Set<Vertex> seen) {

        if (!seen.add(v)) return 0;

        return v.getEdges().stream()
                .filter(edge -> edge.getDirection() == Direction.OUTGOING)
                .map(Edge::getNext)
                .map(vertex -> getDepth(vertex, seen) + 1)
                .reduce(0, (n, m) -> n > m ? n : m);
    }

    public String name;
    LinkedList<Label> labels = new LinkedList<>();
    LinkedList<PropertyPair<Object>> properties = new LinkedList<>();
    LinkedList<Edge> edges = new LinkedList<>();
}
