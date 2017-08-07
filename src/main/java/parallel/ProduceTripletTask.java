package parallel;

import nodeParser.NodeMatch;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Vector;

import static parallel.Manager.*;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ProduceTripletTask implements Runnable {
    @Override
    @SuppressWarnings("unchecked cast")
    public void run() {
        while(!Thread.currentThread().isInterrupted()) {

            StopWatch timer = new StopWatch();
            StopWatch t2 = new StopWatch();
            timer.start();

            ContextEntry contextEntry = contexts.get((int) (Math.random() * contexts.length()));
            Vector<Long> containedIDs = new Vector<>(scopeVectors.get(contextEntry.scope));

            long s1Match = -1;
            long s2Match = -1;

            while (containedIDs.size() > 0) {

                // get random targetID
                long targetID = containedIDs.get((int) (Math.random() * containedIDs.size()));
                containedIDs.remove(targetID);

                // if already in queue then try next target - else add to queue
//                if (!inQueueSet.add(targetID)) continue;

                // if target is the context try next target
                if (targetID == contextEntry.context) {
//                    inQueueSet.remove(targetID);
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
                }

                if (s1Match >= 0 && s2Match >= 0) break;

//                inQueueSet.remove(targetID);
            }

            if (s1Match >= 0 && s2Match >= 0) {
                // match found - add to tripletQueue
//                System.out.println("MATCH");
                Triplet triplet = new Triplet(contextEntry.context, s1Match, s2Match, contextEntry.contextFunction);
                try {
                    t2.start();
                    tripletQueue.put(triplet);
                    t2.stop();
                } catch (InterruptedException e) {
//                    e.printStackTrace();
                    System.out.println("Task on thread " + Thread.currentThread().getId() + " cancelled.");
                }
            } else {
                // no match found - remove nodes from inQueueSet
//                System.out.println("NO MATCH");
//                inQueueSet.remove(s1Match);
//                inQueueSet.remove(s2Match);
            }
            timer.stop();
            producerTimes.add(timer.getNanoTime() - t2.getNanoTime());
        }
    }

    private boolean matchNode(NodeMatch queryNode, Node targetNode) {

//        System.out.println(String.format(
//                "Matching %s with %d",
//                queryNode, targetNode.getId()
//        ));

        // check labels & properties
        if (!queryNode.getLabels().stream().allMatch(targetNode::hasLabel))
            return false;

        return queryNode.getProperties().stream()
                .allMatch(objectPropertyPair ->
                        targetNode.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                                && targetNode.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
    }
}
