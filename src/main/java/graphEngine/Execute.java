package graphEngine;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import parallel.Manager;

import static graphEngine.TestGraphQueries.viewGraph;
import static org.neo4j.procedure.Mode.SCHEMA;
import static org.neo4j.procedure.Mode.WRITE;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.execute", mode = SCHEMA)
    public void execute(@Name("Max no. of interactions") long maxInteractions) {

        Computer SC = new Computer(db);
        SC.compute((int) maxInteractions);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.preProcess", mode = SCHEMA)
    public void preProcess() {

        Computer SC = new Computer(db);
        SC.preProcess();

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadMany", mode = SCHEMA)
    public void loadMany(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgram);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadManyQuery", mode = SCHEMA)
    public void loadManyQuery(@Name("No. of graphs") long graphs) {

        for (int i = 0; i < (int) graphs; i++)
            db.execute(TestGraphQueries.terminatingProgramWithQueryMatching);

        //TODO deal with return
    }

    @Procedure(value = "graphEngine.loadParallel", mode = SCHEMA)
    public void loadParallel(@Name("No of data systems") long dataSystems) {

        // create program
        try (Transaction tx = db.beginTx()) {

            // scopes
            Node main = db.createNode(Components.SCOPE);
            Node computation = db.createNode(Components.SCOPE, Label.label("Computation"));

            // contexts
            Node initializer = db.createNode(Components.CONTEXT);
            initializer.setProperty(Components.function, "INITIALIZE");
            initializer.setProperty(Components.s1Query, "(:Uninitialized)");
            initializer.setProperty(Components.s2Query, "(:Computation)");

            Node binMutate = db.createNode(Components.CONTEXT);
            binMutate.setProperty(Components.function, "BINARYMUTATE");
            binMutate.setProperty(Components.s1Query, "(:Initialized)");
            binMutate.setProperty(Components.s2Query, "(:Initialized)");

            Node onePointCross = db.createNode(Components.CONTEXT);
            onePointCross.setProperty(Components.function, "ONEPOINTCROSS");
            onePointCross.setProperty(Components.s1Query, "(:Initialized)");
            onePointCross.setProperty(Components.s2Query, "(:Initialized)");

            Node uniformCross = db.createNode(Components.CONTEXT);
            uniformCross.setProperty(Components.function, "UNIFORMCROSS");
            uniformCross.setProperty(Components.s1Query, "(:Initialized)");
            uniformCross.setProperty(Components.s2Query, "(:Initialized)");

            Node output = db.createNode(Components.CONTEXT);
            output.setProperty(Components.function, "OUTPUT");
            output.setProperty(Components.s1Query, "(:Fittest)");
            output.setProperty(Components.s2Query, "(:Initialized)");

            main.createRelationshipTo(output, Components.CONTAINS);
            main.createRelationshipTo(initializer, Components.CONTAINS);
            main.createRelationshipTo(computation, Components.CONTAINS);

            computation.createRelationshipTo(binMutate, Components.CONTAINS);
            computation.createRelationshipTo(onePointCross, Components.CONTAINS);
            computation.createRelationshipTo(uniformCross, Components.CONTAINS);

            // data nodes
            for (int i = 0; i < (int) dataSystems; i++) {
                Node data = db.createNode(Label.label("Data"), Label.label("Uninitialized"));
                main.createRelationshipTo(data, Components.CONTAINS);
            }

            // fittest solution
            Node fittest = db.createNode(Label.label("Data"), Label.label("Fittest"));
            main.createRelationshipTo(fittest, Components.CONTAINS);


            tx.success();
        }

    }

    @Procedure(value = "graphEngine.runParallel", mode = SCHEMA)
    public void runParallel(@Name("Max no. of interactions") long maxInteractions) {

        Manager manager = new Manager((int) maxInteractions, db);
        manager.go();
    }

}
