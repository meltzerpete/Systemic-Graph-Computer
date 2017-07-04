package graphEngine;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.*;
import org.neo4j.harness.junit.Neo4jRule;

/**
 * Created by pete on 01/07/17.
 */

public class TripletSelectionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(TripletSelection.class);

    @Test
    public void shouldFindTriplets() throws Throwable {
        try(Driver driver = GraphDatabase.driver( neo4j.boltURI() , Config.build()
                .withEncryptionLevel( Config.EncryptionLevel.NONE ).toConfig() );
            Session session = driver.session() ) {
            session.run( "CREATE (main:System:Scope {name:'main'}), (print:System:Context {name:'print',function:'print'}), (star:System:Context {name:'*', function:'multiply'}), (c1:System:Scope {name:'c1'}), (c2:System:Scope {name:'c2'}), (subtract_e:System:Context {name:'-e', function:'subtract'}), (a1:System {name:'A1'}), (a2:System {name:'A2'}), (a3:System {name:'A3'}), (a4:System {name: 'A4'}),\n" +
                    "(main)-[:CONTAINS]->(print),\n" +
                    "(main)-[:CONTAINS]->(star),\n" +
                    "(main)-[:CONTAINS]->(c1),\n" +
                    "(main)-[:CONTAINS]->(c2),\n" +
                    "(c1)-[:CONTAINS]->(a1),\n" +
                    "(c1)-[:CONTAINS]->(a2),\n" +
                    "(c1)-[:CONTAINS]->(subtract_e),\n" +
                    "(c2)-[:CONTAINS]->(a3),\n" +
                    "(c2)-[:CONTAINS]->(a4),\n" +
                    "(c2)-[:CONTAINS]->(subtract_e),\n" +
                    "(a1)<-[:FITS_1]-(subtract_e)-[:FITS_1]->(a3),\n" +
                    "(a2)<-[:FITS_2]-(subtract_e)-[:FITS_2]->(a4)\n");
            StatementResult result = session.run("CALL graphEngine.selectTriplet()");
            StatementResult resul2 = session.run("CALL graphEngine.selectTriplet()");
            StatementResult resul3 = session.run("CALL graphEngine.selectTriplet()");
        }
    }
}