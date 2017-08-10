package nodeParser;

import org.neo4j.graphdb.Label;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class NodeMatch {

    private ArrayList<Label> labels;
    private ArrayList<PropertyPair> properties;

    public NodeMatch(ArrayList<Label> labels, ArrayList<PropertyPair> properties) {
        this.labels = labels;
        this.properties = properties;
    }

    @SuppressWarnings("unchecked cast")
    public ArrayList<Label> getLabels() {
        return (ArrayList<Label>) labels.clone();
    }

    @SuppressWarnings("unchecked cast")
    public ArrayList<PropertyPair> getProperties() {
        return (ArrayList<PropertyPair>) properties.clone();
    }
}
