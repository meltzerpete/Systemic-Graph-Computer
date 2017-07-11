package originalGraphComponents;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

/**
 * Created by pete on 05/07/17.
 */
public class TestGraphQueries {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

//    private TestGraphQueries(){};

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
                    "(a1)<-[:S1]-(subtract_e)-[:S1]->(a3)," +
                    "(a2)<-[:S2]-(subtract_e)-[:S2]->(a4)";

    public static String unmatchedSystems =
            "CREATE (main:System:Scope)," +
                    "(context:System:Context {s1:['Data','Data1'], s2:['Data','Data2']})," +
                    "(a:System:Scope {data:'12'})," +
                    "(b:System:Context {data:'13'})," +
                    "(c:System:Data1 {data:'14'})," +
                    "(d:System:Data2 {data:'15'})," +
                    "(e:System:Data {data:'16'})," +
                    "(main)-[:CONTAINS]->(context)," +
                    "(main)-[:CONTAINS]->(a)," +
                    "(main)-[:CONTAINS]->(b)," +
                    "(main)-[:CONTAINS]->(c)," +
                    "(main)-[:CONTAINS]->(d)," +
                    "(main)-[:CONTAINS]->(e)";

    public static String systemsWithFunctions =
            "CREATE (:System:Context {function:'NOP', s1:'', s2:''}), (:System {function:'ADD'})";

    public static String systemsWithShapeNodes =
            "CREATE" +
                    "(a1:System {name:'a1', data:8})," +
                    "(a2:System {name:'a2', data:7})," +
                    "(a3:System {name:'a3', data:6})," +
                    "(a4:System {name:'a4', data:5})," +
                    "(main:System:Scope {name:'main'})," +
                    "(subE:System:Context {name:'subE', function:'SUBTRACTe'})," +
                    "(mul:System:Context {name:'mul', function:'MULTIPLY'})," +
                    "(print:System:Context {name:'print', function:'PRINT'})," +
                    "(c1:System:Scope {name:'c1'})," +
                    "(c2:System:Scope {name:'c2'})," +
                    "(data:Shape {name:'data'})," +
                    "(data1:Shape {name:'data1'})," +
                    "(data2:Shape {name:'data2'})," +
                    "(func:Shape {name:'function'})," +
                    "(scope:Shape {name:'scope'})," +
                    "(a1)-[:HAS_SHAPE]->(data1)," +
                    "(a2)-[:HAS_SHAPE]->(data2)," +
                    "(a3)-[:HAS_SHAPE]->(data1)," +
                    "(a4)-[:HAS_SHAPE]->(data2)," +
                    "(a1)-[:HAS_SHAPE]->(data)," +
                    "(a2)-[:HAS_SHAPE]->(data)," +
                    "(a3)-[:HAS_SHAPE]->(data)," +
                    "(a4)-[:HAS_SHAPE]->(data)," +
                    "(main)-[:HAS_SHAPE]->(scope)," +
                    "(main)-[:CONTAINS]->(print)," +
                    "(main)-[:CONTAINS]->(mul)," +
                    "(main)-[:CONTAINS]->(c1)," +
                    "(main)-[:CONTAINS]->(c2)," +
                    "(c1)-[:HAS_SHAPE]->(scope)," +
                    "(c1)-[:CONTAINS]->(subE)," +
                    "(c1)-[:CONTAINS]->(a1)," +
                    "(c1)-[:CONTAINS]->(a2)," +
                    "(c2)-[:HAS_SHAPE]->(scope)," +
                    "(c2)-[:CONTAINS]->(subE)," +
                    "(c2)-[:CONTAINS]->(a3)," +
                    "(c2)-[:CONTAINS]->(a4)," +
                    "(subE)-[:HAS_SHAPE]->(func)," +
                    "(subE)-[:S1]->(data1)," +
                    "(subE)-[:S2]->(data2)," +
                    "(mul)-[:HAS_SHAPE]->(func)," +
                    "(mul)-[:S1]->(data)," +
                    "(mul)-[:S2]->(data)," +
                    "(print)-[:HAS_SHAPE]->(func)," +
                    "(print)-[:S1]->(data)," +
                    "(print)-[:S2]->(data)";

    public static String systemsWithShapeProperties =
            "CREATE" +
                    "(a1:System:Data:Data1 {name:'a1', data:10})," +
                    "(a2:System:Data:Data2 {name:'a2', data:8})," +
                    "(a3:System:Data:Data1 {name:'a3', data:9})," +
                    "(a4:System:Data:Data2 {name:'a4', data:6})," +
                    "(main:System:Scope {name:'main'})," +

                    "(subE:System:Context {name:'subE', function:'SUBTRACTe', " +
                        "s1Labels:['Data1'], s2Labels:['Data2']})," +

                    "(mul:System:Context {name:'mul', function:'MULTIPLY'," +
                        "s1Labels:['Data'], s2Labels:['Data']})," +

                    "(print:System:Context {name:'print', function:'PRINT'," +
                        "s1Labels:['Data'], s2Labels:['Data']})," +

                    "(c1:System:Scope {name:'c1'})," +
                    "(c2:System:Scope {name:'c2'})," +
                    "(main)-[:CONTAINS]->(print)," +
                    "(main)-[:CONTAINS]->(mul)," +
                    "(main)-[:CONTAINS]->(c1)," +
                    "(main)-[:CONTAINS]->(c2)," +
                    "(c1)-[:CONTAINS]->(subE)," +
                    "(c1)-[:CONTAINS]->(a1)," +
                    "(c1)-[:CONTAINS]->(a2)," +
                    "(c2)-[:CONTAINS]->(subE)," +
                    "(c2)-[:CONTAINS]->(a3)," +
                    "(c2)-[:CONTAINS]->(a4)";

    @Procedure(value = "originalGraphEngine.loadGraph", mode = Mode.SCHEMA)
    public void loadGraph() {
        db.execute(systemsWithShapeProperties);
    }
}

// s1Properties as kvPair
// s1Labels
// s1Relationships