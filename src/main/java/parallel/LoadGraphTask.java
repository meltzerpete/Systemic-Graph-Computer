package parallel;

import common.TestGraphQueries;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Created by Pete Meltzer on 30/08/17.
 * <p>Seperate Task used to work around the implicit transaction system
 * in Neo4j procedure calls.</p>
 */
public class LoadGraphTask implements Runnable {

    private GraphDatabaseService db;
    private int numDataSystems;

    /**
     * @param db GraphDatabaseService
     * @param numDataSystems Desired number of solution systems.
     */
    public LoadGraphTask(GraphDatabaseService db, int numDataSystems) {
        this.db = db;
        this.numDataSystems = numDataSystems;
    }

    /**
     * Clears the database and loads the Knapsack program according
     * to the numebr of solution systems specified at instantiation.
     */
    @Override
    public void run() {
        try (Transaction tx = db.beginTx()) {
            db.execute("match (n) detach delete n;");
            TestGraphQueries.knapsack(db, numDataSystems);
            tx.success();
        }
    }
}
