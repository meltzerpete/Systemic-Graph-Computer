package parallel;

import graphEngine.TestGraphQueries;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Created by Pete Meltzer on 30/08/17.
 */
public class LoadGraphTask implements Runnable {

    private GraphDatabaseService db;
    private int numDataSystems;

    public LoadGraphTask(GraphDatabaseService db, int numDataSystems) {
        this.db = db;
        this.numDataSystems = numDataSystems;
    }

    @Override
    public void run() {
        try (Transaction tx = db.beginTx()) {
            TestGraphQueries.knapsack(db, numDataSystems);
            tx.success();
        }
    }
}
