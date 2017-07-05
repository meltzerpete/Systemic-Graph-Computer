package GraphComponents;

/**
 * Created by pete on 05/07/17.
 */
public class TestGraphQueries {

    private TestGraphQueries(){};

    public static String basicSubtraction =
        "CREATE (main:System:Scope {name:'main'})," +
                "(print:System:Context {name:'print', function:'print'})," +
                "(star:System:Context {name:'*', function:'multiply'})," +
                "(c1:System:Scope {name:'c1'}), (c2:System:Scope {name:'c2'})," +
                "(subtract_e:System:Context {name:'-e', function:'subtract'})," +
                "(a1:System {name:'A1', data:7}), (a2:System {name:'A2', data:8})," +
                "(a3:System {name:'A3', data:6}), (a4:System {name:'A4', data:9})," +
                "(main)-[:CONTAINS]->(print)," +
                "(main)-[:CONTAINS]->(star)," +
                "(main)-[:CONTAINS]->(c1)," +
                "(main)-[:CONTAINS]->(c2)," +
                "(c1)-[:CONTAINS]->(a1)," +
                "(c1)-[:CONTAINS]->(a2)," +
                "(c1)-[:CONTAINS]->(subtract_e)," +
                "(c2)-[:CONTAINS]->(a3)," +
                "(c2)-[:CONTAINS]->(a4)," +
                "(c2)-[:CONTAINS]->(subtract_e)," +
                "(a1)<-[:FITS_1]-(subtract_e)-[:FITS_1]->(a3)," +
                "(a2)<-[:FITS_2]-(subtract_e)-[:FITS_2]->(a4)";
}
