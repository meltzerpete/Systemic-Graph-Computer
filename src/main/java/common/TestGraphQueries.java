package common;

import common.Components;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

/**
 * Created by pete on 05/07/17.
 * <p>All graph programs used in testing.</p>
 * <p>The strings may be executed directly as Cypher queries,
 * using db.execute('string') or session.run('string').
 * The methods may be called directly, given the appropriate
 * GraphDatabaseService. The procedures may be called from the
 * cypher-shell or browser interface using the value given in the
 * '@Procedure' annotation.</p>
 */
public class TestGraphQueries {

    @Context
    public GraphDatabaseService db;

    public static String basicSubtraction =
            "CREATE (main:System:SCOPE {key:'main'})," +
                    "(print:System:CONTEXT {key:'print', function:'print'})," +
                    "(star:System:CONTEXT {key:'*', function:'multiply'})," +
                    "(c1:System:SCOPE {key:'c1'}), (c2:System:SCOPE {key:'c2'})," +
                    "(subtract_e:System:CONTEXT {key:'-e', function:'subtract'})," +
                    "(a1:System {key:'A1', data:7}), (a2:System {key:'A2', data:8})," +
                    "(a3:System {key:'A3', data:6}), (a4:System {key:'A4', data:9})," +
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
            "CREATE (main:System:SCOPE)," +
                    "(CONTEXT:System:CONTEXT {s1:['Data','Data1'], s2:['Data','Data2']})," +
                    "(a:System:SCOPE {data:'12'})," +
                    "(b:System:CONTEXT {data:'13'})," +
                    "(c:System:Data1 {data:'14'})," +
                    "(d:System:Data2 {data:'15'})," +
                    "(e:System:Data {data:'16'})," +
                    "(main)-[:CONTAINS]->(CONTEXT)," +
                    "(main)-[:CONTAINS]->(a)," +
                    "(main)-[:CONTAINS]->(b)," +
                    "(main)-[:CONTAINS]->(c)," +
                    "(main)-[:CONTAINS]->(d)," +
                    "(main)-[:CONTAINS]->(e)";

    public static String systemsWithFunctions =
            "CREATE (:System:CONTEXT {function:'NOP', s1:'', s2:''}), (:System {function:'ADD'})";

    public static String systemsWithShapeNodes =
            "CREATE" +
                    "(a1:System {key:'a1', data:8})," +
                    "(a2:System {key:'a2', data:7})," +
                    "(a3:System {key:'a3', data:6})," +
                    "(a4:System {key:'a4', data:5})," +
                    "(main:System:SCOPE {key:'main'})," +
                    "(subE:System:CONTEXT {key:'subE', function:'SUBTRACTe'})," +
                    "(mul:System:CONTEXT {key:'mul', function:'MULTIPLY'})," +
                    "(print:System:CONTEXT {key:'print', function:'PRINT'})," +
                    "(c1:System:SCOPE {key:'c1'})," +
                    "(c2:System:SCOPE {key:'c2'})," +
                    "(data:Shape {key:'data'})," +
                    "(data1:Shape {key:'data1'})," +
                    "(data2:Shape {key:'data2'})," +
                    "(func:Shape {key:'function'})," +
                    "(SCOPE:Shape {key:'SCOPE'})," +
                    "(a1)-[:HAS_SHAPE]->(data1)," +
                    "(a2)-[:HAS_SHAPE]->(data2)," +
                    "(a3)-[:HAS_SHAPE]->(data1)," +
                    "(a4)-[:HAS_SHAPE]->(data2)," +
                    "(a1)-[:HAS_SHAPE]->(data)," +
                    "(a2)-[:HAS_SHAPE]->(data)," +
                    "(a3)-[:HAS_SHAPE]->(data)," +
                    "(a4)-[:HAS_SHAPE]->(data)," +
                    "(main)-[:HAS_SHAPE]->(SCOPE)," +
                    "(main)-[:CONTAINS]->(print)," +
                    "(main)-[:CONTAINS]->(mul)," +
                    "(main)-[:CONTAINS]->(c1)," +
                    "(main)-[:CONTAINS]->(c2)," +
                    "(c1)-[:HAS_SHAPE]->(SCOPE)," +
                    "(c1)-[:CONTAINS]->(subE)," +
                    "(c1)-[:CONTAINS]->(a1)," +
                    "(c1)-[:CONTAINS]->(a2)," +
                    "(c2)-[:HAS_SHAPE]->(SCOPE)," +
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
                    "(a1:System:Data:Data1 {key:'a1', data:10})," +
                    "(a2:System:Data:Data2 {key:'a2', data:8})," +
                    "(a3:System:Data:Data1 {key:'a3', data:9})," +
                    "(a4:System:Data:Data2 {key:'a4', data:6})," +
                    "(main:System:SCOPE {key:'main'})," +

                    "(subE:System:CONTEXT {key:'subE', function:'SUBTRACTe', " +
                    "s1Labels:['Data1'], s2Labels:['Data2']})," +

                    "(mul:System:CONTEXT {key:'mul', function:'MULTIPLY'," +
                    "s1Labels:['Data'], s2Labels:['Data']})," +

                    "(print:System:CONTEXT {key:'print', function:'PRINT'," +
                    "s1Labels:['Data'], s2Labels:['Data']})," +

                    "(c1:System:SCOPE {key:'c1'})," +
                    "(c2:System:SCOPE {key:'c2'})," +
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

    public static String terminatingProgram =
            "CREATE" +
                    "(a1:System:Data:Data1 {key:'a1', data:10})," +
                    "(a2:System:Data:Data2 {key:'a2', data:8})," +
                    "(a3:System:Data:Data1 {key:'a3', data:9})," +
                    "(a4:System:Data:Data2 {key:'a4', data:6})," +
                    "(main:System:SCOPE {key:'main'})," +

                    "(subE:System:CONTEXT {key:'subE', function:'SUBTRACTe', " +
                    "s1Labels:['Data1'], s2Labels:['Data2']})," +

                    "(mul:System:CONTEXT {key:'mul', function:'MULTIPLY'})," +

                    "(print:System:CONTEXT {key:'print', function:'PRINT'})," +

                    "(c1:System:SCOPE {key:'c1'})," +
                    "(c2:System:SCOPE {key:'c2'})," +
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

    public static String programWithQueryMatching =
            "CREATE" +

                    // scopes
                    "(main:SCOPE)," +
                    "(c1:SCOPE)," +
                    "(c2:SCOPE)," +

                    // contexts
                    "(subE:CONTEXT {function:'SUBTRACTe', " +
                    "s1Query:'(:Data1)', s2Query:'(:Data2)'})," +
                    "(mul:CONTEXT {function:'MULTIPLY'," +
                    "s1Query:'(:Data)', s2Query:'(:Data)'})," +
                    "(print:CONTEXT {function:'PRINT'," +
                    "s1Query:'(:Data)', s2Query:'(:Data)'})," +

                    // data systems
                    "(a1:Data:Data1 {data:10})," +
                    "(a2:Data:Data2 {data:8})," +
                    "(a3:Data:Data1 {data:9})," +
                    "(a4:Data:Data2 {data:6})," +

                    // relationships
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

    public static String terminatingProgramWithQueryMatching =
            "CREATE" +

                    // scopes
                    "(main:SCOPE)," +
                    "(c1:SCOPE)," +
                    "(c2:SCOPE)," +

                    // contexts
                    "(subE:System:CONTEXT {function:'SUBTRACTe', " +
                    "s1Query:'(n:Data1)', s2Query:'(n:Data2)'})," +

                    // data systems
                    "(a1:Data:Data1 {data:10})," +
                    "(a2:Data:Data2 {data:8})," +
                    "(a3:Data:Data1 {data:9})," +
                    "(a4:Data:Data2 {data:6})," +

                    // relationships
                    "(main)-[:CONTAINS]->(c1)," +
                    "(main)-[:CONTAINS]->(c2)," +
                    "(c1)-[:CONTAINS]->(subE)," +
                    "(c1)-[:CONTAINS]->(a1)," +
                    "(c1)-[:CONTAINS]->(a2)," +
                    "(c2)-[:CONTAINS]->(subE)," +
                    "(c2)-[:CONTAINS]->(a3)," +
                    "(c2)-[:CONTAINS]->(a4)";

    public static void knapsack(GraphDatabaseService db, int noOfDataSystems) {
        // scopes
        Node main = db.createNode(Components.SCOPE);
        Node computation = db.createNode(Components.SCOPE, Components.COMPUTATION);

        // contexts
        Node initializer = db.createNode(Components.CONTEXT);
        initializer.setProperty(Components.function, "INITIALIZE");
        initializer.setProperty(Components.s1Query, "(:UNINITIALIZED)");
        initializer.setProperty(Components.s2Query, "(:COMPUTATION)");

        Node binMutate = db.createNode(Components.CONTEXT);
        binMutate.setProperty(Components.function, "BINARYMUTATE");
        binMutate.setProperty(Components.s1Query, "(:INITIALIZED)");
        binMutate.setProperty(Components.s2Query, "(:INITIALIZED)");

        Node onePointCross = db.createNode(Components.CONTEXT);
        onePointCross.setProperty(Components.function, "ONEPOINTCROSS");
        onePointCross.setProperty(Components.s1Query, "(:INITIALIZED)");
        onePointCross.setProperty(Components.s2Query, "(:INITIALIZED)");

        Node uniformCross = db.createNode(Components.CONTEXT);
        uniformCross.setProperty(Components.function, "UNIFORMCROSS");
        uniformCross.setProperty(Components.s1Query, "(:INITIALIZED)");
        uniformCross.setProperty(Components.s2Query, "(:INITIALIZED)");

        Node output = db.createNode(Components.CONTEXT);
        output.setProperty(Components.function, "OUTPUT");
        output.setProperty(Components.s1Query, "(:FITTEST)");
        output.setProperty(Components.s2Query, "(:INITIALIZED)");

        main.createRelationshipTo(output, Components.CONTAINS);
        main.createRelationshipTo(initializer, Components.CONTAINS);
        main.createRelationshipTo(computation, Components.CONTAINS);

        computation.createRelationshipTo(binMutate, Components.CONTAINS);
        computation.createRelationshipTo(onePointCross, Components.CONTAINS);
        computation.createRelationshipTo(uniformCross, Components.CONTAINS);

        // data nodes
        for (int i = 0; i < noOfDataSystems; i++) {
            Node data = db.createNode(Components.DATA, Components.UNINITIALIZED);
            main.createRelationshipTo(data, Components.CONTAINS);
        }

        // fittest solution
        Node fittest = db.createNode(Components.DATA, Components.FITTEST);
        main.createRelationshipTo(fittest, Components.CONTAINS);
    }

    public static String viewGraph = "MATCH (n)" +
            "OPTIONAL MATCH (n)-[r]->(m)" +
            "RETURN DISTINCT id(n) AS ID, labels(n) AS Labels," +
            "properties(n) AS Properties, {r:r, n:id(m)} AS Relationships";

    @Procedure(value = "graphEngine.loadGraph", mode = Mode.SCHEMA)
    public void loadGraph() {
        db.execute(systemsWithShapeProperties);
    }

    @Procedure(value = "graphEngine.loadTerminatingGraph", mode = Mode.SCHEMA)
    public void loadTerminatingGraph() {
        db.execute(terminatingProgram);
    }
}