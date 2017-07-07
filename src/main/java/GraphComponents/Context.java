package GraphComponents;

import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Property;

/**
 * Created by pete on 04/07/17.
 */

@NodeEntity(label = "Context")
public class Context extends SCSystem {

    @Property(name = "function")
    private Function function;

    @Property(name = "s1")
    private int schema1;

    @Property(name = "s2")
    private int schema2;

}
