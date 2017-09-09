package nodeParser;

import org.neo4j.graphdb.Label;

import java.util.LinkedList;

/**
 * Created by Pete Meltzer on 05/08/17.
 * <p>NodeMatch object is a composition of the Labels and Properties
 * that may be used for comparison against a target Node.</p>
 */
public class NodeMatch {

    private LinkedList<Label> labels;
    private LinkedList<PropertyPair> properties;

    /**
     * Create a new NodeMatch object.
     * @param labels LinkedList of Labels
     * @param properties LinkedList of PropertyPairs
     */
    public NodeMatch(LinkedList<Label> labels, LinkedList<PropertyPair> properties) {
        this.labels = labels;
        this.properties = properties;
    }

    /**
     * Returns a LinkedList of all Labels.
     * @return LinkedList of Labels
     */
    @SuppressWarnings("unchecked cast")
    public LinkedList<Label> getLabels() {
        return (LinkedList<Label>) labels.clone();
    }

    /**
     * Returns a LinkedList of properties as PropertyPairs.
     * @return LinkedList of PropertyPairs
     */
    @SuppressWarnings("unchecked cast")
    public LinkedList<PropertyPair> getProperties() {
        return (LinkedList<PropertyPair>) properties.clone();
    }
}
