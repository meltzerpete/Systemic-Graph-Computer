package parallel;

import graphEngine.Components;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;

import static graphEngine.TestGraphQueries.viewGraph;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ManagerTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void goTest() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE)
                .toConfig());
             Session session = driver.session()) {
            
//            neo4j.withConfig("LockManager", "community");
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            // create program
            int noOfDataSystems = 50;
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
                for (int i = 0; i < noOfDataSystems; i++) {
                    Node data = db.createNode(Label.label("Data"), Label.label("Uninitialized"));
                    main.createRelationshipTo(data, Components.CONTAINS);
                }

                // fittest solution
                Node fittest = db.createNode(Label.label("Data"), Label.label("Fittest"));
                main.createRelationshipTo(fittest, Components.CONTAINS);


                tx.success();
            }


            Manager.go(db);
//            System.out.println(db.execute(viewGraph).resultAsString());
        }

    }

    @Test
    public void singleTest() throws Throwable {
        try (Driver driver = GraphDatabase.driver(neo4j.boltURI(), Config.build()
                .withEncryptionLevel(Config.EncryptionLevel.NONE)
                .toConfig());
             Session session = driver.session()) {

//            neo4j.withConfig("LockManager", "community");
            GraphDatabaseService db = neo4j.getGraphDatabaseService();

            // create program
            int noOfDataSystems = 1000;
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
                for (int i = 0; i < noOfDataSystems; i++) {
                    Node data = db.createNode(Label.label("Data"), Label.label("Uninitialized"));
                    main.createRelationshipTo(data, Components.CONTAINS);
                }

                // fittest solution
                Node fittest = db.createNode(Label.label("Data"), Label.label("Fittest"));
                main.createRelationshipTo(fittest, Components.CONTAINS);


                tx.success();
            }


            Single.go(db);
//            System.out.println(db.execute(viewGraph).resultAsString());
        }

    }

}