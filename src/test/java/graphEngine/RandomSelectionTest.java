package graphEngine;

import GraphComponents.TestGraphQueries;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by pete on 05/07/17.
 */
public class RandomSelectionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(ID.class);

    @Test
    public void shouldReturnRandomElementOfHashSet() throws Throwable {
        System.out.println("Random selection testing: ");

        HashSet<Integer> set = new HashSet<>();
        set.add(5);
        set.add(2);
        set.add(7);
        set.add(4);
        set.add(5);

        System.out.println(set.size());

        System.out.println(random(set));
        System.out.println(random(set));
        System.out.println(random(set));
        System.out.println(random(set));
        System.out.println(random(set));
    }

    public static <T> T random(Collection<T> coll) {
        int num = (int) (Math.random() * coll.size());
        for(T t: coll) if (--num < 0) return t;
        throw new AssertionError();
    }
}