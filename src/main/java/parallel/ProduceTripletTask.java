package parallel;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;

import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;

import static parallel.Manager.*;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ProduceTripletTask implements Runnable {
    @Override
    @SuppressWarnings("unchecked cast")
    public void run() {
        HashSet<Long> visited = new HashSet<>();
        while(run) {

            // TODO - is there a deadlock opportunity here!?

            ContextEntry contextEntry = contextArray[ThreadLocalRandom.current().nextInt(contextArray.length)];
            Long[] containedIDs = scopeContainedIDs.get(contextEntry.scope);

            long s1Match = -1;
            long s2Match = -1;

            visited.clear();
            int visitedCount = 0;

            while (containedIDs.length > visitedCount) {

                // get random targetID
                long targetID = containedIDs[ThreadLocalRandom.current().nextInt(containedIDs.length)];
                if (!visited.add(targetID)) continue;
                visitedCount++;

                // if target is the context try next target
                if (targetID == contextEntry.context) {
                    continue;
                }

                try (Transaction tx = db.beginTx()) {
                    // S1
                    // check node matches properties, labels etc.
                    if (s1Match < 0 && matchNode(contextEntry.s1, db.getNodeById(targetID))) {
                        // match
                        s1Match = targetID;
                        tx.success();
                        continue;   // prevent s1 and s2 getting same target
                    }

                    // S2
                    // check node matches properties, labels etc.
                    if (s2Match < 0 && matchNode(contextEntry.s2, db.getNodeById(targetID))) {
                        // match
                        s2Match = targetID;
                    }
                    tx.success();
                } catch (DatabaseShutdownException e) {
                    if (run) e.printStackTrace();
                    // otherwise ignore exception
                }

                if (s1Match >= 0 && s2Match >= 0) break;
            }

            if (s1Match >= 0 && s2Match >= 0) {
                // match found - add to tripletQueue
                Triplet triplet = new Triplet(contextEntry, s1Match, s2Match);
                try {
                    tripletQueue.put(triplet);
                } catch (InterruptedException e) {
                    System.out.println("Task on thread " + Thread.currentThread().getId() + " interrupted.");
                }
            }
        }
    }
}
