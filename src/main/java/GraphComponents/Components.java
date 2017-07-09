package GraphComponents;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Created by pete on 09/07/17.
 */
public class Components {

    public static final Label readyLabel = Label.label("Ready");
    public static final RelationshipType fits1 = RelationshipType.withName("FITS_1");
    public static final RelationshipType fits2 = RelationshipType.withName("FITS_2");
    public static final RelationshipType contains = RelationshipType.withName("CONTAINS");
}
