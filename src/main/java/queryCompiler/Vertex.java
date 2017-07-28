package queryCompiler;

import org.neo4j.graphdb.Label;

import java.util.LinkedList;

/**
 * Created by Pete Meltzer on 25/07/17.
 */
public class Vertex {

    public LinkedList<Label> getLabels() {
        return labels;
    }

    public LinkedList<PropertyPair<Object>> getProperties() {
        return properties;
    }

    public LinkedList<Edge> getEdges() {
        return edges;
    }

    public int getDepth() { return depth; }

    public String name;
    LinkedList<Label> labels = new LinkedList<>();
    LinkedList<PropertyPair<Object>> properties = new LinkedList<>();
    LinkedList<Edge> edges = new LinkedList<>();

    int depth;
}
