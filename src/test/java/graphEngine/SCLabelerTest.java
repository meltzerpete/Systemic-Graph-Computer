package graphEngine;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by Pete Meltzer on 12/07/17.
 */
public class SCLabelerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void getInstance() throws Throwable {
       Computer comp = new Computer(neo4j.getGraphDatabaseService());
       SCLabeler SClabeler = comp.getLabeler();

    }
}