package queryCompiler;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.LinkedList;

/**
 * Created by Pete Meltzer on 25/07/17.
 */
public class Edge {

    public RelationshipType getType() {
        return type;
    }

    public LinkedList<PropertyPair<Object>> getProperties() {
        return properties;
    }

    public Vertex getNext() {
        return next;
    }

    public Direction getDirection() {
        return direction;
    }

    RelationshipType type;
    LinkedList<PropertyPair<Object>> properties = new LinkedList<>();
    Vertex next;
    Direction direction;
}
