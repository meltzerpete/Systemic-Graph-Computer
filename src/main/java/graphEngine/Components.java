package graphEngine;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
class Components {
    private Components(){}

    static final Label CONTEXT = Label.label("CONTEXT");
    /**
     * Use {@code WriteLocks} instead.
     */
    @Deprecated // use WriteLocks instead
    static final Label PROCESSING = Label.label("PROCESSING");
    static final Label READY = Label.label("READY");
    static final Label SCOPE = Label.label("SCOPE");

    static final RelationshipType CONTAINS = RelationshipType.withName("CONTAINS");
    static final RelationshipType FITS1 = RelationshipType.withName("FITS1");
    static final RelationshipType FITS2 = RelationshipType.withName("FITS2");

    //Property strings
    @Deprecated
    static final String s1Labels = "s1Labels";
    @Deprecated
    static final String s2Labels = "s2Labels";

    static final String s1Query = "s1Query";
    static final String s2Query = "s2Query";
    static final String readyContextScopeID = "rcsID";

    static final String data = "data";
    static final String function = "function";

}
