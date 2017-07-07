package GraphComponents;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

/**
 * Created by pete on 04/07/17.
 */

@NodeEntity(label = "SCSystem")
public class SCSystem {

    @GraphId
    private Long id;

    @Property(name = "S1")
    private int schema1;

    @Property(name = "function")
    private Function function;

    @Property(name = "S2")
    private int schema2;

}
