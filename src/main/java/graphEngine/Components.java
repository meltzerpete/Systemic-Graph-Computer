package graphEngine;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Components {
    private Components(){}

    public static final Label CONTEXT = Label.label("CONTEXT");
    /**
     * Use {@code WriteLocks} instead.
     */
    @Deprecated // use WriteLocks instead
    public static final Label PROCESSING = Label.label("PROCESSING");
    public static final Label READY = Label.label("READY");
    public static final Label SCOPE = Label.label("SCOPE");

    public static final RelationshipType CONTAINS = RelationshipType.withName("CONTAINS");
    public static final RelationshipType FITS1 = RelationshipType.withName("FITS1");
    public static final RelationshipType FITS2 = RelationshipType.withName("FITS2");

    //Property strings
    @Deprecated
    static final String s1Labels = "s1Labels";
    @Deprecated
    static final String s2Labels = "s2Labels";

    public static final String s1Query = "s1Query";
    public static final String s2Query = "s2Query";
    public static final String readyContextScopeID = "rcsID";

    public static final String data = "data";
    public static final String function = "function";

    // for probability model
    public static final String distributionA = "fDistA";
    public static final String distributionB = "fDistB";

    public static final Label FITTEST = Label.label("fittest");

    public static final String probability = "prob";

}
