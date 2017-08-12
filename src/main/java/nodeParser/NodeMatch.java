package nodeParser;

import org.neo4j.graphdb.Label;

import java.util.LinkedList;
import java.util.LinkedList;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class NodeMatch {

    private LinkedList<Label> labels;
    private LinkedList<PropertyPair> properties;

    public NodeMatch(LinkedList<Label> labels, LinkedList<PropertyPair> properties) {
        this.labels = labels;
        this.properties = properties;
    }

    @SuppressWarnings("unchecked cast")
    public LinkedList<Label> getLabels() {
        return (LinkedList<Label>) labels.clone();
    }

    @SuppressWarnings("unchecked cast")
    public LinkedList<PropertyPair> getProperties() {
        return (LinkedList<PropertyPair>) properties.clone();
    }
}
