package parallel;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ProduceTripletTask implements Runnable {

    public ProduceTripletTask(Manager manager) {
        this.manager = manager;
    }

    private final Manager manager;

    @Override
    @SuppressWarnings("unchecked cast")
    public void run() {
        HashSet<Long> visited = new HashSet<>();

        try (Transaction tx = manager.db.beginTx()) {

            while(manager.run.get()) {

                ContextEntry contextEntry = manager.contextArray[ThreadLocalRandom.current().nextInt(manager.contextArray.length)];
                Long[] containedIDs = manager.nodesContainedInScope.get(contextEntry.scope);

                long s1Match = -1;
                long s2Match = -1;

                visited.clear();
                int visitedCount = 0;

                while (visitedCount < containedIDs.length) {

                    // get random targetID
                    long targetID = containedIDs[ThreadLocalRandom.current().nextInt(containedIDs.length)];
                    if (!visited.add(targetID)) continue;

                    visitedCount++;

                    // if target is the context try next target
                    if (targetID == contextEntry.context) {
                        continue;
                    }

                    // S1
                    // check node matches properties, labels etc.
                    Node s1Node = manager.db.getNodeById(targetID);
                    if (s1Match < 0 && manager.matchNode(contextEntry.s1, s1Node)) {
                        // match
                        s1Match = targetID;
                        continue;   // prevent s1 and s2 getting same target
                    }

                    // S2
                    // check node matches properties, labels etc.
                    Node s2Node = manager.db.getNodeById(targetID);
                    if (s2Match < 0 && manager.matchNode(contextEntry.s2, s2Node)) {
                        // match
                        s2Match = targetID;
                    }

                    if (s1Match >= 0 && s2Match >= 0) break;
                }

                if (s1Match >= 0 && s2Match >= 0) {
                    // match found - add to tripletQueue
                    Triplet triplet = new Triplet(contextEntry, s1Match, s2Match);
                    try {
                        manager.tripletQueue.put(triplet);
                    } catch (InterruptedException e) {
                        System.out.println("Task on thread " + Thread.currentThread().getId() + " interrupted.");
                    }
                }
            }
        }
    }
}
