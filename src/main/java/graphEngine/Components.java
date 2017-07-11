package graphEngine;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Components {
    private Components(){};

    final Label CONTEXT = Label.label("CONTEXT");
    final Label PROCESSING = Label.label("PROCESSING");
    final Label READY = Label.label("READY");
    final Label SCOPE = Label.label("SCOPE");
    final RelationshipType CONTAINS = RelationshipType.withName("CONTAINS");
    final RelationshipType FITS1 = RelationshipType.withName("FITS1");
    final RelationshipType FITS2 = RelationshipType.withName("FITS2");
}
