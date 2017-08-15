package probability;

import graphEngine.Components;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import static org.neo4j.procedure.Mode.SCHEMA;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
public class Execute {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;


    @Procedure(value = "graphEngine.loadProbability", mode = SCHEMA)
    public void loadProbability(@Name("No of data systems") long dataSystems,
                                @Name("Probability of fittest selection") long nFittest) {

        // create program
        try (Transaction tx = db.beginTx()) {

            // scopes
            Node main = db.createNode(Components.SCOPE);

            // data nodes
            for (int i = 0; i < (int) dataSystems; i++) {
                Node data = db.createNode(Label.label("Data"));
                data.setProperty(Components.distributionA, new int[]{1,0,0,0,0});
                data.setProperty(Components.distributionB, new int[]{1,0,0,0,0});
                main.createRelationshipTo(data, Components.CONTAINS);
            }

            // fittest solution
            Node fittest = db.createNode(Label.label("Data"), Components.FITTEST);
            fittest.setProperty(Components.probability, (int) nFittest);
            fittest.setProperty(Components.distributionA, new int[]{0,0,0,0,1});
            fittest.setProperty(Components.distributionB, new int[]{0,0,0,0,1});
            main.createRelationshipTo(fittest, Components.CONTAINS);


            tx.success();
        }

    }

    @Procedure(value = "graphEngine.runProbability", mode = SCHEMA)
    public void runProbability(@Name("Max no. of interactions") long maxInteractions) {

        Manager manager = new Manager((int) maxInteractions, db);
        manager.go();
    }

}
