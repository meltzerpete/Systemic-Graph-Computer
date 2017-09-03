package queryCompiler;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.RelationshipType;

import java.util.LinkedList;

/**
 * Created by Pete Meltzer on 25/07/17.
 */
class Edge {

    @Override
    public String toString() {
        return "-(" + this.next.name + ")";
    }

    public RelationshipType getType() {
        return type;
    }

    @SuppressWarnings("unchecked cast")
    public LinkedList<PropertyPair<Object>> getProperties() {
        return (LinkedList<PropertyPair<Object>>) properties.clone();
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
